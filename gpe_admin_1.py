#!/usr/bin/env python3
"""
Wikidata First-Level Administrative Division Extractor (Local Two-Pass)
======================================================================
Extracts first-level admin divisions using the P150 ("contains 
administrative territorial entity") property explicitly linking them 
to known countries, strictly from the local dump.

Phase 1: Scans for known GPEs and extracts their P150 division QIDs.
Phase 2: Scans for those specific division QIDs and extracts data.

Filtering (1850-present)
-------------------------
* P150 Strict Relationship (Resolves cycling race false positives)
* P576 / P582 temporal checks
* Gloss-based date heuristic
* Native Homograph Resolution (.gpe_entry)
"""

import ctypes
import json
import multiprocessing
import os
import re
import signal
import sys
import time
from multiprocessing import Array, Pool, Value, cpu_count
from typing import Optional

try:
    import orjson
except ImportError:
    sys.exit("⚠️  orjson is required.  Install with:  pip install orjson")

# ========== CONFIGURATION ==========

BASE_DIR       = "/Volumes/Extreme Pro/FKG/SHACL-API-Docker"
DUMP_PATH      = "/Volumes/Extreme Pro/FKG/latest-all.json"
GPE_ENTITY_TTL = os.path.join(BASE_DIR, "gpe_entity.ttl")
OUTPUT_DIR     = BASE_DIR
LOG_DIR        = os.path.join(BASE_DIR, "logs")

MAX_WORKERS    = max(1, cpu_count() - 2)
PROGRESS_SEC   = 5
MAX_LINE_BYTES = 200 * 1024 * 1024
READ_BUFFER    = 16 * 1024 * 1024

YEAR_CUTOFF = 1850

BLOCKLIST_QIDS = frozenset({})

_HISTORICAL_GLOSS_KW = frozenset({
    "historical", "historic", "former", "ancient", "medieval",
    "extinct", "dynasty", "empire", "kingdom", "caliphate", "khanate",
    "abolished", "dissolved", "defunct", "ceased", "merged",
    "predecessor", "succeeded by", "protectorate", "colony",
    "countship", "duchy", "principality", "republic (",
})

for d in [LOG_DIR]:
    os.makedirs(d, exist_ok=True)

# ========== LOAD PARENT COUNTRIES ==========

def _load_country_map(ttl_path: str) -> dict[str, str]:
    qid_to_safe_label: dict[str, str] = {}
    _qid_re = re.compile(r":identifier wiki:(Q\d+)")
    _label_re = re.compile(r'rdfs:label "(.+?) \(entity\)"@en')

    with open(ttl_path, encoding="utf-8") as f:
        content = f.read()

    for block in content.split("\n\n"):
        if ".entity.01 a :Geopolitical_Entity" not in block: continue
        qid_m = _qid_re.search(block)
        label_m = _label_re.search(block)
        if qid_m and label_m:
            qid = qid_m.group(1)
            safe = re.sub(r"[^A-Za-z0-9_]", "", label_m.group(1).replace(" ", "_"))
            qid_to_safe_label[qid] = safe
    return qid_to_safe_label

# ========== HELPERS ==========

_SAFE_RE = re.compile(r"[^A-Za-z0-9_]")
_YEAR_RE = re.compile(r"\b(\d{3,4})\b")

def sanitize_uri(text: str) -> str:
    return _SAFE_RE.sub("", text.replace(" ", "_"))

def escape_turtle(s: str) -> str:
    if not s: return ""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

def format_eta(seconds: float) -> str:
    if seconds < 0 or seconds > 604_800: return "estimating…"
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"

def fmt_bytes(b: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024: return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"

def _extract_wikidata_year(time_str: str) -> Optional[int]:
    if not time_str: return None
    m = re.match(r"[+-]?(\d{1,4})", time_str)
    return int(m.group(1)) if m else None

def _get_claim_year(claim: dict) -> Optional[int]:
    try: return _extract_wikidata_year(claim["mainsnak"]["datavalue"]["value"]["time"])
    except (KeyError, TypeError): return None

def _get_qualifier_year(claim: dict, qual_prop: str) -> Optional[int]:
    try:
        for q in claim.get("qualifiers", {}).get(qual_prop, []):
            yr = _extract_wikidata_year(q.get("datavalue", {}).get("value", {}).get("time"))
            if yr is not None: return yr
    except (KeyError, TypeError): pass
    return None

def _setup_log(prefix: str) -> str:
    return os.path.join(LOG_DIR, f"{prefix}_{time.strftime('%Y%m%d_%H%M%S')}.log")

# ========== SHARED STATE ==========

_shutdown_flag: Optional[Value] = None
_shared_bytes:  Optional[Array] = None
_shared_scanned: Optional[Array] = None
_shared_hits: Optional[Array] = None
_shared_filtered: Optional[Array] = None

_country_map: dict[str, str] = {}
_division_map: dict[str, str] = {}

_QID_PATTERN = re.compile(rb'"id":"(Q\d+)"')

# ========== PHASE 1: P150 HIERARCHY SCAN ==========

def _init_phase1(shutdown_flag, s_bytes, s_scanned, country_map):
    global _shutdown_flag, _shared_bytes, _shared_scanned, _country_map
    _shutdown_flag = shutdown_flag
    _shared_bytes = s_bytes
    _shared_scanned = s_scanned
    _country_map = country_map
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def _worker_phase1(args: tuple) -> dict:
    range_start, range_end, worker_id = args
    idx = worker_id
    local_p150_map = {}
    local_bytes = 0
    local_scanned = 0
    lines_since_flush = 0

    with open(DUMP_PATH, "rb", buffering=READ_BUFFER) as f:
        f.seek(range_start)
        while True:
            if _shutdown_flag and _shutdown_flag.value: break
            pos_before = f.tell()
            if pos_before >= range_end: break
            
            raw = f.readline()
            if not raw: break
            
            line_len = len(raw)
            local_bytes += line_len
            local_scanned += 1
            if line_len > MAX_LINE_BYTES: continue

            qid_match = _QID_PATTERN.search(raw[:200])
            if not qid_match: continue
            
            qid_str = qid_match.group(1).decode('ascii')
            
            if qid_str in _country_map:
                # FIX: Strip trailing commas for valid JSON
                line = raw.strip()
                if line.endswith(b","): line = line[:-1]
                
                try:
                    entity = orjson.loads(line)
                    div_qids = []
                    for claim in entity.get("claims", {}).get("P150", []):
                        try:
                            val_id = claim["mainsnak"]["datavalue"]["value"]["id"]
                            div_qids.append(val_id)
                        except (KeyError, TypeError): pass
                    if div_qids:
                        local_p150_map[qid_str] = div_qids
                except Exception:
                    pass

            lines_since_flush += 1
            if lines_since_flush >= 50000:
                _shared_bytes[idx] = local_bytes
                _shared_scanned[idx] = local_scanned
                lines_since_flush = 0

    _shared_bytes[idx] = local_bytes
    _shared_scanned[idx] = local_scanned
    return local_p150_map

# ========== PHASE 2: DATA EXTRACTION ==========

def _init_phase2(shutdown_flag, s_bytes, s_scanned, s_hits, s_filtered, country_map, division_map):
    global _shutdown_flag, _shared_bytes, _shared_scanned, _shared_hits, _shared_filtered, _country_map, _division_map
    _shutdown_flag = shutdown_flag
    _shared_bytes = s_bytes
    _shared_scanned = s_scanned
    _shared_hits = s_hits
    _shared_filtered = s_filtered
    _country_map = country_map
    _division_map = division_map
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def _check_admin_and_filter(entity: dict, parent_qid: str) -> tuple[Optional[dict], Optional[str]]:
    qid = entity.get("id", "")
    claims = entity.get("claims", {})
    label = (entity.get("labels") or {}).get("en", {}).get("value", "")
    gloss = (entity.get("descriptions") or {}).get("en", {}).get("value", "")

    if qid in BLOCKLIST_QIDS: return None, f"BLOCKLIST  {qid}"
    if not label: return None, f"NO_LABEL  {qid}"

    parent_safe = _country_map.get(parent_qid)
    if not parent_safe: return None, f"UNKNOWN_PARENT  {qid}"

    # Temporal filters
    dissolved_year, p31_end_year = None, None
    for claim in claims.get("P576", [])[:5]:
        if yr := _get_claim_year(claim): dissolved_year = max(dissolved_year or 0, yr)
    for claim in claims.get("P31", [])[:10]:
        if ey := _get_qualifier_year(claim, "P582"): p31_end_year = max(p31_end_year or 0, ey)

    end_year = None
    if dissolved_year and p31_end_year: end_year = max(dissolved_year, p31_end_year)
    elif dissolved_year: end_year = dissolved_year
    elif p31_end_year: end_year = p31_end_year

    if end_year is not None and end_year < YEAR_CUTOFF:
        return None, f"PRE_{YEAR_CUTOFF}_P576  {qid}  end={end_year}"

    if end_year is None and gloss:
        gloss_lower = gloss.lower()
        if any(kw in gloss_lower for kw in _HISTORICAL_GLOSS_KW):
            years = [int(y) for y in _YEAR_RE.findall(gloss) if 100 < int(y) < 2100]
            if years and max(years) < YEAR_CUTOFF:
                return None, f"PRE_{YEAR_CUTOFF}_GLOSS  {qid} gloss_max={max(years)}"

    names = {label}
    for a in (entity.get("aliases") or {}).get("en", []):
        if v := a.get("value"): names.add(v)

    inception = None
    for claim in claims.get("P571", [])[:3]:
        if yr := _get_claim_year(claim):
            inception = yr
            break

    return {
        "qid": qid, "primary_label": label, "gloss": gloss,
        "parent_safe": parent_safe, "parent_qid": parent_qid,
        "names": list(names), "inception": inception, "dissolved": end_year,
    }, None

def _worker_phase2(args: tuple) -> dict:
    range_start, range_end, worker_id = args
    idx = worker_id
    results, filter_lines = [], []
    local_bytes, local_scanned, local_hits, local_filtered = 0, 0, 0, 0
    lines_since_flush = 0

    with open(DUMP_PATH, "rb", buffering=READ_BUFFER) as f:
        f.seek(range_start)
        while True:
            if _shutdown_flag and _shutdown_flag.value: break
            pos_before = f.tell()
            if pos_before >= range_end: break
            
            raw = f.readline()
            if not raw: break
            
            line_len = len(raw)
            local_bytes += line_len
            local_scanned += 1
            if line_len > MAX_LINE_BYTES: continue

            # Regex Fast-Path checking the Division Map built in Phase 1
            qid_match = _QID_PATTERN.search(raw[:200])
            if not qid_match: continue
            
            qid_str = qid_match.group(1).decode('ascii')
            parent_qid = _division_map.get(qid_str)
            if not parent_qid: continue

            # FIX: Strip trailing commas for valid JSON
            line = raw.strip()
            if line.endswith(b","): line = line[:-1]

            # Only full JSON parse if it's a confirmed P150 division
            try:
                entity = orjson.loads(line)
                data, filter_reason = _check_admin_and_filter(entity, parent_qid)
                if data:
                    results.append(data)
                    local_hits += 1
                elif filter_reason:
                    local_filtered += 1
                    filter_lines.append(f"{filter_reason}\n")
            except Exception: pass

            lines_since_flush += 1
            if lines_since_flush >= 50000:
                _shared_bytes[idx] = local_bytes
                _shared_scanned[idx] = local_scanned
                _shared_hits[idx] = local_hits
                _shared_filtered[idx] = local_filtered
                lines_since_flush = 0

    _shared_bytes[idx] = local_bytes
    _shared_scanned[idx] = local_scanned
    _shared_hits[idx] = local_hits
    _shared_filtered[idx] = local_filtered
    return {"results": results, "hits": local_hits, "filtered": local_filtered, "filter_lines": filter_lines}

# ========== ORCHESTRATOR ==========

def _compute_ranges(file_size: int, n_workers: int) -> list[tuple[int, int]]:
    chunk = file_size // n_workers
    ranges = []
    with open(DUMP_PATH, "rb") as f:
        start = 0
        for i in range(n_workers):
            if i == n_workers - 1: ranges.append((start, file_size))
            else:
                f.seek(start + chunk)
                f.readline()
                end = f.tell()
                ranges.append((start, end))
                start = end
    return ranges

def _print_dashboard(phase_name, n_workers, file_size, t0):
    elapsed = time.time() - t0
    total_bytes = sum(_shared_bytes[i] for i in range(n_workers))
    total_scanned = sum(_shared_scanned[i] for i in range(n_workers))
    
    # Safely aggregate hits and filtered
    total_hits = sum(_shared_hits[i] for i in range(n_workers)) if _shared_hits else 0
    total_filtered = sum(_shared_filtered[i] for i in range(n_workers)) if _shared_filtered else 0
    
    pct = total_bytes / file_size * 100 if file_size else 0
    rate = total_bytes / elapsed if elapsed > 0 else 0
    eta = (file_size - total_bytes) / rate if rate > 0 else 0

    lines = [
        "─" * 74,
        f"  ▶ {phase_name}  |  {pct:5.1f}%  |  ETA {format_eta(eta)}  |  {fmt_bytes(rate)}/s",
        f"  📊 Scanned: {total_scanned:>14,}  |  Hits: {total_hits:<6}  |  Filtered: {total_filtered:<6}",
        "─" * 74
    ]
    sys.stderr.write("\n".join(lines) + "\n")
    sys.stderr.flush()

def run_pipeline():
    file_size = os.path.getsize(DUMP_PATH)
    n_workers = MAX_WORKERS
    ranges = _compute_ranges(file_size, n_workers)
    country_map = _load_country_map(GPE_ENTITY_TTL)

    print(f"\n📂 Dump: {DUMP_PATH} ({fmt_bytes(file_size)})")
    print(f"🌍 Known GPEs: {len(country_map)}\n")

    # Initialize shared memory globally
    global _shutdown_flag, _shared_bytes, _shared_scanned, _shared_hits, _shared_filtered
    _shutdown_flag = Value(ctypes.c_int, 0)
    _shared_bytes = Array(ctypes.c_longlong, n_workers)
    _shared_scanned = Array(ctypes.c_longlong, n_workers)
    _shared_hits = Array(ctypes.c_longlong, n_workers)
    _shared_filtered = Array(ctypes.c_longlong, n_workers)

    interrupted = False
    def _sigint_handler(signum, frame):
        nonlocal interrupted
        interrupted = True
        _shutdown_flag.value = 1
        sys.stderr.write("\n🛑 Ctrl-C — stopping workers...\n")
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, _sigint_handler)

    # --- PHASE 1 ---
    print("========================================")
    print(" PHASE 1/2: Resolving P150 Hierarchies")
    print("========================================")
    for i in range(n_workers): 
        _shared_bytes[i] = _shared_scanned[i] = _shared_hits[i] = _shared_filtered[i] = 0
        
    work1 = [(r[0], r[1], i) for i, r in enumerate(ranges)]
    
    division_map = {}
    t0 = time.time()
    
    with Pool(n_workers, initializer=_init_phase1, initargs=(_shutdown_flag, _shared_bytes, _shared_scanned, country_map)) as pool:
        async_results = [pool.apply_async(_worker_phase1, (w,)) for w in work1]
        while True:
            if all(ar.ready() for ar in async_results): break
            _print_dashboard("PHASE 1", n_workers, file_size, t0)
            time.sleep(PROGRESS_SEC)
        for ar in async_results:
            try:
                for c_qid, div_list in ar.get().items():
                    for d_qid in div_list:
                        division_map[d_qid] = c_qid
            except Exception: pass

    if interrupted: sys.exit(1)
    print(f"\n✅ Phase 1 Complete. Found {len(division_map)} explicitly declared P150 divisions.\n")

    # --- PHASE 2 ---
    print("========================================")
    print(" PHASE 2/2: Extracting Division Data")
    print("========================================")
    for i in range(n_workers): 
        _shared_bytes[i] = _shared_scanned[i] = _shared_hits[i] = _shared_filtered[i] = 0
        
    work2 = [(r[0], r[1], i) for i, r in enumerate(ranges)]
    
    all_results = []
    filter_log_path = _setup_log("admin_filtered")
    t0 = time.time()

    with Pool(n_workers, initializer=_init_phase2, initargs=(_shutdown_flag, _shared_bytes, _shared_scanned, _shared_hits, _shared_filtered, country_map, division_map)) as pool:
        async_results = [pool.apply_async(_worker_phase2, (w,)) for w in work2]
        while True:
            if all(ar.ready() for ar in async_results): break
            _print_dashboard("PHASE 2", n_workers, file_size, t0)
            time.sleep(PROGRESS_SEC)
        for ar in async_results:
            try:
                res = ar.get()
                all_results.extend(res["results"])
                if res["filter_lines"]:
                    with open(filter_log_path, "a") as ff:
                        ff.writelines(res["filter_lines"])
            except Exception: pass

    signal.signal(signal.SIGINT, original_sigint)
    print(f"\n✅ Phase 2 Complete. Extracted {len(all_results)} valid administrative divisions.")
    return all_results

# ========== TTL GENERATION ==========

ENTITY_HEADER = """\
@prefix :      <https://falcontologist.github.io/shacl-demo/ontology/> .
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix wiki:  <http://www.wikidata.org/entity/> .

# ==========================================
# FIRST-LEVEL ADMINISTRATIVE DIVISIONS
# ==========================================

"""

ENTRY_HEADER = """\
@prefix :      <https://falcontologist.github.io/shacl-demo/ontology/> .
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .

# ==========================================
# FIRST-LEVEL ADMINISTRATIVE DIVISION ENTRIES
# ==========================================

"""

def generate_ttls(records: list[dict]) -> None:
    entity_file = os.path.join(OUTPUT_DIR, "admin_entity.ttl")
    entry_file  = os.path.join(OUTPUT_DIR, "admin_entry.ttl")

    seen: dict[str, dict] = {}
    for r in records: seen.setdefault(r["qid"], r)
    unique = sorted(seen.values(), key=lambda r: r["primary_label"])

    # --- Entity file ---
    parts: list[str] = [ENTITY_HEADER]
    for r in unique:
        safe = sanitize_uri(r["primary_label"])
        block = f":{safe}.entity.01 a :Geopolitical_Entity ;\n"
        block += f'    rdfs:label "{escape_turtle(r["primary_label"])} (entity)"@en ;\n'
        if r["gloss"]: block += f'    :gloss "{escape_turtle(r["gloss"])}"@en ;\n'
        block += f'    :part_of :{r["parent_safe"]}.entity.01 ;\n'
        block += f'    :identifier wiki:{r["qid"]} ;\n'
        block += f'    :source <https://www.wikidata.org/> .\n\n'
        parts.append(block)

    with open(entity_file, "w", encoding="utf-8") as f:
        f.write("".join(parts))
    print(f"✅ Wrote {entity_file}  ({len(unique)} entities)")

    # --- Entry file (Native .gpe_entry Consolidation) ---
    qid_to_label = {r["qid"]: r["primary_label"] for r in unique}
    name_to_qids: dict[str, set[str]] = {}
    
    for r in unique:
        for name in r["names"]:
            name_to_qids.setdefault(name, set()).add(r["qid"])

    parts = [ENTRY_HEADER]
    counter = 0
    for name in sorted(name_to_qids):
        safe = sanitize_uri(name)
        if not safe: continue
        
        sense_uris = [f":{sanitize_uri(qid_to_label[q])}.entity.01" for q in sorted(name_to_qids[name]) if q in qid_to_label]
        if not sense_uris: continue
        
        block = f":{safe}.gpe_entry a :Geopolitical_Entry ;\n"
        block += f'    rdfs:label "{escape_turtle(name)}"@en ;\n'
        block += f'    :sense {", ".join(sense_uris)} ;\n'
        block += f'    :pos :Noun.entity.01 ;\n'
        block += f'    :source <https://www.wikidata.org/> .\n\n'
        parts.append(block)
        counter += 1

    with open(entry_file, "w", encoding="utf-8") as f:
        f.write("".join(parts))
    print(f"✅ Wrote {entry_file}  ({counter} unique strings consolidated)")

# ========== MAIN ==========

def main() -> None:
    if not os.path.exists(DUMP_PATH): sys.exit(f"❌ Dump not found at {DUMP_PATH}")
    if not os.path.exists(GPE_ENTITY_TTL): sys.exit(f"❌ Country file not found at {GPE_ENTITY_TTL}")

    records = run_pipeline()
    if records:
        generate_ttls(records)
        parent_counts: dict[str, int] = {}
        for r in records: parent_counts[r["parent_safe"]] = parent_counts.get(r["parent_safe"], 0) + 1
        print(f"\n📊 Top 20 countries by division count:")
        for label, cnt in sorted(parent_counts.items(), key=lambda x: -x[1])[:20]:
            print(f"   {cnt:>5}  {label}")

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    main()