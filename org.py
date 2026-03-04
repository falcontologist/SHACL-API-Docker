#!/usr/bin/env python3
"""
Wikidata Organization Extractor (Two-Phase, P108-Driven)
=========================================================
Phase 1: Scans every entity for P108 ("employer") claims and collects the
          *target* QIDs — these are real organisations that real people work at.
          This relationship-driven approach is far cleaner than P31/Q43229
          (which catches bowling clubs and Wikimedia projects).  Entities that
          notable people list as their employer naturally bias toward the most
          relevant organisations in finance, government, and international news.
Phase 2: Scans again for those specific QIDs and extracts label, gloss,
          and aliases into FKG-compatible Turtle files.  Applies P31-based
          exclusion filters, gloss heuristics, and the underscore-URI guard
          learned from the Person run.

Architecture
------------
* orjson for fast JSON parsing
* Byte-range chunking with multiprocessing.Pool (spawn)
* Shared-memory live dashboard with ETA, errors, throughput per phase
* Ctrl-C graceful shutdown with checkpoint resume
* MAX_LINE_BYTES guard against memory blowouts
* Per-line try/except — never stalls on dirty data

Output
------
* organization_entity.ttl  — :Organization_Entity instances  (.organization_entity.NN)
* organization_entry.ttl   — :Organization_Entry instances   (.organization_entry)
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

BASE_DIR    = "/Volumes/Extreme Pro/FKG/SHACL-API-Docker"
DUMP_PATH   = "/Volumes/Extreme Pro/FKG/latest-all.json"
OUTPUT_DIR  = BASE_DIR
LOG_DIR     = os.path.join(BASE_DIR, "logs")
CHECKPOINT  = os.path.join(BASE_DIR, "organization_checkpoint.json")

MAX_WORKERS    = max(1, cpu_count() - 2)
PROGRESS_SEC   = 5
MAX_LINE_BYTES = 200 * 1024 * 1024
READ_BUFFER    = 16 * 1024 * 1024

# ========== EXCLUSION QIDs ==========
# If a P108 target has any of these P31 types, skip it.
# These catch the noise that leaks through P108: geographic entities, humans,
# fictional entities, Wikimedia meta-pages, and overly generic concepts.

EXCLUSION_TYPE_QIDS = frozenset({
    # Fictional / mythological
    "Q95074",      # fictional character
    "Q15632617",   # fictional entity
    "Q22674925",   # fictional object
    "Q14514600",   # fictional organization
    "Q11073431",   # mythological character
    # Wikimedia noise
    "Q4167410",    # Wikimedia disambiguation page
    "Q13442814",   # scholarly article
    "Q4167836",    # Wikimedia category
    "Q11266439",   # Wikimedia template
    "Q15184295",   # Wikimedia module
    "Q17633526",   # Wikinews article
    "Q4663903",    # Wikipedia:Featured articles
    # Geographic — not organizations
    "Q5",          # human
    "Q515",        # city
    "Q6256",       # country
    "Q3624078",    # sovereign state
    "Q23397",      # lake
    "Q2386087",    # reservoir
    "Q82794",      # geographic region
    "Q15642541",   # human-geographic territory
    "Q35657",      # U.S. state
    "Q10864048",   # first-level admin division
    # Products / works — not organizations
    "Q7889",       # video game
    "Q11424",      # film
    "Q5398426",    # TV series
    "Q7725634",    # literary work
    "Q482994",     # album
    "Q134556",     # single (music)
    "Q35127",      # website
})

BLOCKLIST_QIDS = frozenset({
    # Populate as you spot noise in filter logs
})

# ========== GLOSS HEURISTICS ==========
# If the English description contains any of these substrings, filter it out.

GLOSS_REJECT_SUBSTRINGS = (
    "fictional",
    "mythological",
    "legendary",
    "character in ",
    "character from ",
    "video game",
    "wikimedia",
    "wikipedia",
    "disambiguation",
)

# ========== HELPERS ==========

_SAFE_RE = re.compile(r"[^A-Za-z0-9_]")
_JUNK_ALIAS_RE = re.compile(r"^[\s*_\-=<>\[\]{}()|/\\#@!$%^&+=~`]+$")
_MAX_ALIAS_LEN = 200

def sanitize_uri(text: str) -> str:
    return _SAFE_RE.sub("", text.replace(" ", "_"))

def escape_turtle(s: str) -> str:
    if not s:
        return ""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

def _clean_alias(name: str) -> Optional[str]:
    name = name.strip().strip("*_`~").strip()
    if not name or len(name) <= 1 or len(name) > _MAX_ALIAS_LEN:
        return None
    if _JUNK_ALIAS_RE.match(name):
        return None
    return name

def _is_blank_uri(safe: str) -> bool:
    """Guard against underscore-only URIs (non-Latin labels with no ASCII)."""
    return not safe or safe.strip("_") == ""

def format_eta(seconds: float) -> str:
    if seconds < 0 or seconds > 604_800:
        return "estimating…"
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def fmt_bytes(b: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"

def _get_claim_qid(claim: dict) -> Optional[str]:
    try:
        return claim["mainsnak"]["datavalue"]["value"]["id"]
    except (KeyError, TypeError):
        return None

def _setup_log(prefix: str) -> str:
    return os.path.join(LOG_DIR, f"{prefix}_{time.strftime('%Y%m%d_%H%M%S')}.log")

# ========== CHECKPOINT ==========

def _save_checkpoint(data: dict):
    tmp = CHECKPOINT + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, CHECKPOINT)

def _load_checkpoint() -> Optional[dict]:
    if os.path.exists(CHECKPOINT):
        try:
            with open(CHECKPOINT) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None
    return None

# ========== SHARED STATE ==========

_shutdown_flag:   Optional[Value] = None
_shared_bytes:    Optional[Array] = None
_shared_scanned:  Optional[Array] = None
_shared_hits:     Optional[Array] = None
_shared_filtered: Optional[Array] = None
_shared_errors:   Optional[Array] = None

# Phase 2 needs the target set
_org_qids: frozenset = frozenset()

_QID_PATTERN = re.compile(rb'"id":"(Q\d+)"')
_P108_FAST   = b'"P108"'   # employer — the discovery property

# ========== PHASE 1: Collect organisation QIDs via P108 (employer) ==========

def _init_phase1(shutdown_flag, s_bytes, s_scanned, s_hits, s_errors):
    global _shutdown_flag, _shared_bytes, _shared_scanned, _shared_hits, _shared_errors
    _shutdown_flag  = shutdown_flag
    _shared_bytes   = s_bytes
    _shared_scanned = s_scanned
    _shared_hits    = s_hits
    _shared_errors  = s_errors
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def _worker_phase1(args: tuple) -> dict:
    """Scan byte-range. Find entities with P108 (employer) and collect target org QIDs."""
    range_start, range_end, worker_id = args
    idx = worker_id
    org_targets: set[str] = set()
    local_bytes = local_scanned = local_hits = local_errors = 0
    lines_since_flush = 0

    with open(DUMP_PATH, "rb", buffering=READ_BUFFER) as f:
        f.seek(range_start)
        while True:
            if _shutdown_flag and _shutdown_flag.value:
                break
            if f.tell() >= range_end:
                break

            try:
                raw = f.readline()
            except Exception:
                local_errors += 1
                continue

            if not raw:
                break

            local_bytes += len(raw)
            local_scanned += 1

            # Unconditional flush every 10k lines — keeps dashboard alive
            lines_since_flush += 1
            if lines_since_flush >= 10_000:
                _shared_bytes[idx]   = local_bytes
                _shared_scanned[idx] = local_scanned
                _shared_hits[idx]    = local_hits
                _shared_errors[idx]  = local_errors
                lines_since_flush = 0

            if len(raw) > MAX_LINE_BYTES:
                continue

            # Fast-path: skip lines without P108 (employer)
            if _P108_FAST not in raw:
                continue

            qid_match = _QID_PATTERN.search(raw[:200])
            if not qid_match:
                continue

            line = raw.strip()
            if line.endswith(b","):
                line = line[:-1]

            try:
                entity = orjson.loads(line)
                for claim in entity.get("claims", {}).get("P108", []):
                    tgt = _get_claim_qid(claim)
                    if tgt:
                        org_targets.add(tgt)
                        local_hits += 1
            except Exception:
                local_errors += 1

    # Final flush
    _shared_bytes[idx]   = local_bytes
    _shared_scanned[idx] = local_scanned
    _shared_hits[idx]    = local_hits
    _shared_errors[idx]  = local_errors
    return {"targets": list(org_targets), "hits": local_hits, "errors": local_errors}

# ========== PHASE 2: Extract organisation data ==========

def _init_phase2(shutdown_flag, s_bytes, s_scanned, s_hits, s_filtered, s_errors, org_qids):
    global _shutdown_flag, _shared_bytes, _shared_scanned, _shared_hits, _shared_filtered, _shared_errors, _org_qids
    _shutdown_flag  = shutdown_flag
    _shared_bytes   = s_bytes
    _shared_scanned = s_scanned
    _shared_hits    = s_hits
    _shared_filtered = s_filtered
    _shared_errors  = s_errors
    _org_qids       = org_qids
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def _check_organization(entity: dict) -> tuple[Optional[dict], Optional[str]]:
    """Extract organisation data or return a filter reason."""
    qid = entity.get("id", "")
    claims = entity.get("claims", {})
    label = (entity.get("labels") or {}).get("en", {}).get("value", "")
    gloss = (entity.get("descriptions") or {}).get("en", {}).get("value", "")

    if qid in BLOCKLIST_QIDS:
        return None, f"BLOCKLIST  {qid}"
    if not label:
        return None, f"NO_LABEL  {qid}"

    # P31 exclusion check
    p31_qids: set[str] = set()
    for claim in claims.get("P31", [])[:30]:
        cq = _get_claim_qid(claim)
        if cq:
            p31_qids.add(cq)
    if p31_qids & EXCLUSION_TYPE_QIDS:
        return None, f"EXCLUDED_TYPE  {qid}  types={p31_qids & EXCLUSION_TYPE_QIDS}"

    # Gloss-based heuristic — catch stragglers
    gloss_lower = gloss.lower()
    for kw in GLOSS_REJECT_SUBSTRINGS:
        if kw in gloss_lower:
            return None, f"GLOSS_REJECT  {qid}  keyword={kw!r}  gloss={gloss!r}"

    # Clean primary label
    cleaned_label = _clean_alias(label)
    if not cleaned_label:
        return None, f"BAD_LABEL  {qid}  raw={label!r}"

    # Guard against underscore-only URIs
    safe = sanitize_uri(cleaned_label)
    if _is_blank_uri(safe):
        return None, f"BLANK_URI  {qid}  raw={label!r}"

    # Clean names (label + aliases)
    names: set[str] = set()
    names.add(cleaned_label)

    for a in (entity.get("aliases") or {}).get("en", []):
        v = a.get("value")
        if v:
            cleaned = _clean_alias(v)
            if cleaned and not _is_blank_uri(sanitize_uri(cleaned)):
                names.add(cleaned)

    return {
        "qid": qid,
        "primary_label": cleaned_label,
        "gloss": gloss,
        "names": list(names),
    }, None

def _worker_phase2(args: tuple) -> dict:
    """Scan byte-range, extract data for QIDs in the organisation target set."""
    range_start, range_end, worker_id = args
    idx = worker_id
    results: list[dict] = []
    filter_lines: list[str] = []
    error_lines: list[str] = []
    local_bytes = local_scanned = local_hits = local_filtered = local_errors = 0
    lines_since_flush = 0

    with open(DUMP_PATH, "rb", buffering=READ_BUFFER) as f:
        f.seek(range_start)
        while True:
            if _shutdown_flag and _shutdown_flag.value:
                break
            if f.tell() >= range_end:
                break

            try:
                raw = f.readline()
            except Exception:
                local_errors += 1
                continue

            if not raw:
                break

            local_bytes += len(raw)
            local_scanned += 1

            # Unconditional flush every 10k lines
            lines_since_flush += 1
            if lines_since_flush >= 10_000:
                _shared_bytes[idx]    = local_bytes
                _shared_scanned[idx]  = local_scanned
                _shared_hits[idx]     = local_hits
                _shared_filtered[idx] = local_filtered
                _shared_errors[idx]   = local_errors
                lines_since_flush = 0

            if len(raw) > MAX_LINE_BYTES:
                continue

            qid_match = _QID_PATTERN.search(raw[:200])
            if not qid_match:
                continue

            qid_str = qid_match.group(1).decode("ascii")
            if qid_str not in _org_qids:
                continue

            line = raw.strip()
            if line.endswith(b","):
                line = line[:-1]

            try:
                entity = orjson.loads(line)
            except Exception as exc:
                local_errors += 1
                error_lines.append(f"JSON_ERROR  {qid_str}  {exc}\n")
                continue

            try:
                data, filter_reason = _check_organization(entity)
                if data:
                    results.append(data)
                    local_hits += 1
                elif filter_reason:
                    local_filtered += 1
                    filter_lines.append(f"{filter_reason}\n")
            except Exception as exc:
                local_errors += 1
                error_lines.append(f"FILTER_ERROR  {qid_str}  {exc}\n")

    # Final flush
    _shared_bytes[idx]    = local_bytes
    _shared_scanned[idx]  = local_scanned
    _shared_hits[idx]     = local_hits
    _shared_filtered[idx] = local_filtered
    _shared_errors[idx]   = local_errors
    return {
        "results": results,
        "hits": local_hits,
        "filtered": local_filtered,
        "errors": local_errors,
        "filter_lines": filter_lines,
        "error_lines": error_lines,
    }

# ========== ORCHESTRATOR ==========

def _compute_ranges(file_size: int, n_workers: int) -> list[tuple[int, int]]:
    chunk = file_size // n_workers
    ranges: list[tuple[int, int]] = []
    with open(DUMP_PATH, "rb") as f:
        start = 0
        for i in range(n_workers):
            if i == n_workers - 1:
                ranges.append((start, file_size))
            else:
                f.seek(start + chunk)
                f.readline()
                end = f.tell()
                ranges.append((start, end))
                start = end
    return ranges

def _reset_shared(n_workers: int):
    for i in range(n_workers):
        _shared_bytes[i] = _shared_scanned[i] = _shared_hits[i] = _shared_errors[i] = 0
        if _shared_filtered:
            _shared_filtered[i] = 0

def _print_dashboard(phase_name: str, n_workers: int, file_size: int, t0: float,
                     show_filtered: bool = False):
    elapsed = time.time() - t0
    total_bytes   = sum(_shared_bytes[i]   for i in range(n_workers))
    total_scanned = sum(_shared_scanned[i] for i in range(n_workers))
    total_hits    = sum(_shared_hits[i]    for i in range(n_workers))
    total_errors  = sum(_shared_errors[i]  for i in range(n_workers))

    pct  = total_bytes / file_size * 100 if file_size else 0
    rate = total_bytes / elapsed if elapsed > 0 else 0
    eta  = (file_size - total_bytes) / rate if rate > 0 else 0

    detail = f"  ✅ Hits: {total_hits:>10,}"
    if show_filtered and _shared_filtered:
        total_filtered = sum(_shared_filtered[i] for i in range(n_workers))
        detail += f"  |  🚫 Filtered: {total_filtered:>8,}"
    detail += f"  |  ⚠️  Errors: {total_errors:>6,}"

    lines = [
        "",
        "─" * 78,
        f"  ▶ {phase_name}  |  {pct:5.1f}%  |  ETA {format_eta(eta)}  |  elapsed {format_eta(elapsed)}",
        f"  📊 Scanned {total_scanned:>14,} lines  |  {fmt_bytes(total_bytes)} / {fmt_bytes(file_size)}  |  {fmt_bytes(rate)}/s",
        detail,
        "─" * 78,
    ]
    sys.stderr.write("\n".join(lines) + "\n")
    sys.stderr.flush()


def run_pipeline() -> list[dict]:
    file_size = os.path.getsize(DUMP_PATH)
    n_workers = MAX_WORKERS
    ranges    = _compute_ranges(file_size, n_workers)

    print(f"\n📂 Dump: {DUMP_PATH} ({fmt_bytes(file_size)})")
    print(f"🔧 Workers: {n_workers}\n")

    # Checkpoint: skip Phase 1 if we already have the target set
    checkpoint = _load_checkpoint()
    org_qids: set[str] = set()

    global _shutdown_flag, _shared_bytes, _shared_scanned, _shared_hits, _shared_filtered, _shared_errors
    _shutdown_flag   = Value(ctypes.c_int, 0)
    _shared_bytes    = Array(ctypes.c_longlong, n_workers)
    _shared_scanned  = Array(ctypes.c_longlong, n_workers)
    _shared_hits     = Array(ctypes.c_longlong, n_workers)
    _shared_filtered = Array(ctypes.c_longlong, n_workers)
    _shared_errors   = Array(ctypes.c_longlong, n_workers)

    interrupted = False
    def _sigint_handler(signum, frame):
        nonlocal interrupted
        interrupted = True
        _shutdown_flag.value = 1
        sys.stderr.write("\n🛑 Ctrl-C — stopping workers…\n")
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, _sigint_handler)

    # ─── PHASE 1: Collect organisation QIDs via P108 (employer) ───
    if checkpoint and "org_qids" in checkpoint:
        org_qids = set(checkpoint["org_qids"])
        print(f"♻️  Phase 1 cached: {len(org_qids):,} organisation QIDs from checkpoint\n")
    else:
        print("=" * 60)
        print(" PHASE 1/2: Scanning for P108 targets (organisation QIDs)")
        print("   Discovery: entities referenced as someone's employer")
        print("=" * 60)
        _reset_shared(n_workers)
        work = [(r[0], r[1], i) for i, r in enumerate(ranges)]
        t0 = time.time()

        with Pool(n_workers, initializer=_init_phase1,
                  initargs=(_shutdown_flag, _shared_bytes, _shared_scanned,
                            _shared_hits, _shared_errors)) as pool:
            async_results = [pool.apply_async(_worker_phase1, (w,)) for w in work]
            while not all(ar.ready() for ar in async_results):
                _print_dashboard("PHASE 1 — P108 Scan", n_workers, file_size, t0)
                time.sleep(PROGRESS_SEC)
            _print_dashboard("PHASE 1 — P108 Scan", n_workers, file_size, t0)

            total_p1_errors = 0
            for ar in async_results:
                try:
                    res = ar.get(timeout=120)
                    org_qids.update(res["targets"])
                    total_p1_errors += res["errors"]
                except Exception as exc:
                    sys.stderr.write(f"⚠️  Phase 1 worker error: {exc}\n")

        elapsed = time.time() - t0
        print(f"\n✅ Phase 1 complete in {format_eta(elapsed)}.")
        print(f"   Unique organisation QIDs found: {len(org_qids):,}")
        print(f"   Errors: {total_p1_errors:,}\n")

        if interrupted:
            _save_checkpoint({
                "org_qids": list(org_qids),
                "phase1_time": format_eta(elapsed),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "interrupted": True,
            })
            print("💾 Partial checkpoint saved. Re-run to resume from Phase 2.")
            sys.exit(1)

        # Save checkpoint so Phase 1 doesn't repeat
        _save_checkpoint({
            "org_qids": list(org_qids),
            "phase1_time": format_eta(elapsed),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        })

    if not org_qids:
        print("❌ No organisation QIDs found. Nothing to extract.")
        return []

    # ─── PHASE 2: Extract organisation entity data ───
    print("=" * 60)
    print(" PHASE 2/2: Extracting organisation data for target QIDs")
    print(f"   Targets: {len(org_qids):,} organisation QIDs")
    print("=" * 60)
    _shutdown_flag.value = 0
    _reset_shared(n_workers)

    # Convert to frozenset for O(1) lookups in workers
    org_qids_frozen = frozenset(org_qids)

    work = [(r[0], r[1], i) for i, r in enumerate(ranges)]
    all_results: list[dict] = []
    filter_log_path = _setup_log("organization_filtered")
    error_log_path  = _setup_log("organization_errors")
    t0 = time.time()

    with Pool(n_workers, initializer=_init_phase2,
              initargs=(_shutdown_flag, _shared_bytes, _shared_scanned,
                        _shared_hits, _shared_filtered, _shared_errors,
                        org_qids_frozen)) as pool:
        async_results = [pool.apply_async(_worker_phase2, (w,)) for w in work]
        while not all(ar.ready() for ar in async_results):
            _print_dashboard("PHASE 2 — Extract", n_workers, file_size, t0,
                             show_filtered=True)
            time.sleep(PROGRESS_SEC)
        _print_dashboard("PHASE 2 — Extract", n_workers, file_size, t0,
                         show_filtered=True)

        total_p2_errors = 0
        for ar in async_results:
            try:
                res = ar.get(timeout=120)
                all_results.extend(res["results"])
                total_p2_errors += res["errors"]
                if res["filter_lines"]:
                    with open(filter_log_path, "a", encoding="utf-8") as ff:
                        ff.writelines(res["filter_lines"])
                if res["error_lines"]:
                    with open(error_log_path, "a", encoding="utf-8") as ef:
                        ef.writelines(res["error_lines"])
            except multiprocessing.TimeoutError:
                sys.stderr.write("⚠️  Phase 2 worker timed out — skipping\n")
            except Exception as exc:
                sys.stderr.write(f"⚠️  Phase 2 worker error: {exc}\n")

    signal.signal(signal.SIGINT, original_sigint)

    elapsed = time.time() - t0
    print(f"\n✅ Phase 2 complete in {format_eta(elapsed)}.")
    print(f"   Extracted: {len(all_results):,} organisations")
    print(f"   Errors:    {total_p2_errors:,}")
    if os.path.exists(filter_log_path):
        print(f"   Filter log → {filter_log_path}")
    if os.path.exists(error_log_path):
        print(f"   Error log  → {error_log_path}")
    return all_results

# ========== TTL GENERATION ==========

ENTITY_HEADER = """\
@prefix :      <https://falcontologist.github.io/shacl-demo/ontology/> .
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix wiki:  <http://www.wikidata.org/entity/> .

# ==========================================
# TOP-LEVEL CLASSES & PROPERTIES
# ==========================================

:Entity a rdfs:Class .
:Entry a rdfs:Class .

:Organization_Entity rdfs:subClassOf :Entity .
:Organization_Entry  rdfs:subClassOf :Entry .

:sense a rdfs:Property .
:pos a rdfs:Property .
:gloss a rdfs:Property .
:identifier a rdfs:Property .
:source a rdfs:Property .

# Part of Speech Entities
:Noun.entity.01 a :Entity ;
    rdfs:label "Noun"@en .

# ==========================================
# ORGANIZATION ENTITIES
# ==========================================

"""

ENTRY_HEADER = """\
@prefix :      <https://falcontologist.github.io/shacl-demo/ontology/> .
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .

# ==========================================
# ORGANIZATION ENTRIES
# ==========================================

"""


def generate_ttls(records: list[dict]) -> None:
    entity_file = os.path.join(OUTPUT_DIR, "organization_entity.ttl")
    entry_file  = os.path.join(OUTPUT_DIR, "organization_entry.ttl")

    # Deduplicate by QID
    seen: dict[str, dict] = {}
    for r in records:
        seen.setdefault(r["qid"], r)
    unique = sorted(seen.values(), key=lambda r: r["primary_label"])

    # --- Entity file (.organization_entity.NN with collision resolution) ---
    uri_counter: dict[str, int] = {}

    def _next_entity_uri(safe_label: str) -> str:
        n = uri_counter.get(safe_label, 1)
        uri_counter[safe_label] = n + 1
        return f":{safe_label}.organization_entity.{n:02d}"

    qid_to_uri: dict[str, str] = {}

    parts: list[str] = [ENTITY_HEADER]
    for r in unique:
        safe = sanitize_uri(r["primary_label"])
        if _is_blank_uri(safe):
            continue

        entity_uri = _next_entity_uri(safe)
        qid_to_uri[r["qid"]] = entity_uri

        block = f"{entity_uri} a :Organization_Entity ;\n"
        block += f'    rdfs:label "{escape_turtle(r["primary_label"])} (entity)"@en ;\n'
        if r["gloss"]:
            block += f'    :gloss "{escape_turtle(r["gloss"])}"@en ;\n'
        block += f'    :identifier wiki:{r["qid"]} ;\n'
        block += f"    :source <https://www.wikidata.org/> .\n\n"
        parts.append(block)

    with open(entity_file, "w", encoding="utf-8") as f:
        f.write("".join(parts))
    print(f"✅ Wrote {entity_file}  ({len(qid_to_uri):,} entities)")

    # --- Entry file ---
    name_to_qids: dict[str, set[str]] = {}
    for r in unique:
        if r["qid"] not in qid_to_uri:
            continue
        for name in r["names"]:
            if not _is_blank_uri(sanitize_uri(name)):
                name_to_qids.setdefault(name, set()).add(r["qid"])

    parts = [ENTRY_HEADER]
    counter = 0
    for name in sorted(name_to_qids):
        safe = sanitize_uri(name)
        if _is_blank_uri(safe):
            continue

        sense_uris = [
            qid_to_uri[q]
            for q in sorted(name_to_qids[name])
            if q in qid_to_uri
        ]
        if not sense_uris:
            continue

        block = f":{safe}.organization_entry a :Organization_Entry ;\n"
        block += f'    rdfs:label "{escape_turtle(name)}"@en ;\n'
        block += f'    :sense {", ".join(sense_uris)} ;\n'
        block += f"    :pos :Noun.entity.01 ;\n"
        block += f"    :source <https://www.wikidata.org/> .\n\n"
        parts.append(block)
        counter += 1

    with open(entry_file, "w", encoding="utf-8") as f:
        f.write("".join(parts))
    print(f"✅ Wrote {entry_file}  ({counter:,} unique entry strings)")

    collisions = sum(1 for v in uri_counter.values() if v > 2)
    if collisions:
        print(f"   ℹ️  {collisions:,} labels needed .organization_entity.02+ disambiguation")

# ========== MAIN ==========

def main() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)

    if not os.path.exists(DUMP_PATH):
        sys.exit(f"❌ Dump not found at {DUMP_PATH}")

    records = run_pipeline()
    if records:
        generate_ttls(records)
    else:
        print("No organisation records extracted.")

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    main()