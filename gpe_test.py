#!/usr/bin/env python3
"""
Optimised Wikidata GPE extractor  (v4 — 1850-present filter)
=============================================================
Extracts countries / sovereign-states (Q6256, Q3624078) from the full
Wikidata `latest-all.json` dump and writes Turtle (TTL) ontology files.

Filtering strategy (1850-present)
----------------------------------
1. **Wikidata temporal properties** — each entity is checked for:
   - P576 (dissolved/abolished)  → if dissolved before 1850, exclude
   - P582 (end time) on P31      → if the "instance of country" claim
     has an end-time before 1850, exclude
   - P571 (inception)            → informational, not used to exclude
     (a country founded in 1200 that still exists should be kept)

2. **Gloss-based date extraction** — many historical entities have date
   ranges in their description like "(750–1258)".  If the latest year
   mentioned is before 1850, the entity is excluded.

3. **Subnational exclusion** — provinces, autonomous communities, and
   constituent parts of larger states are excluded (Buenos Aires Province,
   Catalonia, Hesse, etc.) unless they are also sovereign states.

4. **No-label exclusion** — entities with no English label (Unknown_Qxxx)
   are excluded.

5. **Curated blocklist** — a small set of QIDs for entities that slip
   through the above filters but are clearly not relevant (e.g. fictional
   states, blockchain "countries", Wikimedia disambiguation pages).

All filtering decisions are logged so you can audit what was dropped.

Speed & robustness (unchanged from v3)
---------------------------------------
*  True parallel parsing with orjson + shared-memory progress
*  Real-time dashboard, error log, large-line guard
*  Graceful Ctrl-C, checkpoint/resume

Requirements
------------
    pip install orjson
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
OUTPUT_DIR     = BASE_DIR
LOG_DIR        = os.path.join(BASE_DIR, "logs")

IS_TEST_RUN    = False
TEST_LIMIT     = 500
MAX_WORKERS    = max(1, cpu_count() - 2)
PROGRESS_SEC   = 5
MAX_LINE_BYTES = 200 * 1024 * 1024
READ_BUFFER    = 16 * 1024 * 1024

CHECKPOINT_PATH = os.path.join(BASE_DIR, "gpe_checkpoint.json")

# ========== TEMPORAL FILTER ==========

YEAR_CUTOFF = 1850   # exclude entities whose latest date is before this

# ========== ENTITY TYPE FILTER ==========

# Q6256 = country · Q3624078 = sovereign state
GPE_TYPES = frozenset({"Q6256", "Q3624078"})

# Subnational types to EXCLUDE even if they also have P31=country
# (e.g. "constituent country of the Kingdom of the Netherlands")
SUBNATIONAL_TYPES = frozenset({
    "Q10864048",   # first-level administrative country subdivision
    "Q1221156",    # province of Argentina
    "Q2577883",    # autonomous community of Spain
    "Q1620908",    # country of the United Kingdom
    "Q3336843",    # constituent country of the Kingdom of the Netherlands
    "Q35657",      # state of the United States
    "Q10742",      # autonomous region
})

# ========== CURATED BLOCKLIST ==========
# QIDs of entities that pass automated filters but are clearly noise.
# Each has a comment explaining why.

BLOCKLIST_QIDS = frozenset({
    "Q126282254",  # Joseon Cybernation — blockchain company, not a country
    "Q3479157",    # Serendip — Wikimedia disambiguation page
    "Q2480041",    # Italy in the Middle Ages — historical concept, not a state
    "Q5185064",    # House of Zhu — imperial house, not a country
    "Q2",          # Earth — not a GPE under continents
    "Q131588685",  # Unknown — no English label, no gloss
    "Q2362063",    # Unknown — no English label, no gloss
})

# ========== CONTINENTS ==========

CONTINENTS = {
    "Q2":   "Earth",
    "Q15":  "Africa",
    "Q51":  "Antarctica",
    "Q48":  "Asia",
    "Q46":  "Europe",
    "Q49":  "North_America",
    "Q538": "Oceania",
    "Q18":  "South_America",
}

for d in [LOG_DIR]:
    os.makedirs(d, exist_ok=True)


# ========== LOGGING ==========

def _setup_log(prefix: str) -> str:
    ts = time.strftime("%Y%m%d_%H%M%S")
    return os.path.join(LOG_DIR, f"{prefix}_{ts}.log")

ERROR_LOG_PATH  = _setup_log("gpe_errors")
FILTER_LOG_PATH = _setup_log("gpe_filtered")


# ========== HELPERS ==========

_SAFE_RE = re.compile(r"[^A-Za-z0-9_]")
_YEAR_RE = re.compile(r"\b(\d{3,4})\b")

def sanitize_uri(text: str) -> str:
    return _SAFE_RE.sub("", text.replace(" ", "_"))

def escape_turtle(s: str) -> str:
    if not s:
        return ""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

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


def _extract_wikidata_year(time_str: str) -> Optional[int]:
    """Parse a Wikidata time value like '+1918-00-00T00:00:00Z' → 1918."""
    if not time_str:
        return None
    m = re.match(r"[+-]?(\d{1,4})", time_str)
    if m:
        return int(m.group(1))
    return None


def _get_claim_year(claim: dict, prop: str = None) -> Optional[int]:
    """Extract year from a claim's mainsnak time value."""
    try:
        tv = claim["mainsnak"]["datavalue"]["value"]["time"]
        return _extract_wikidata_year(tv)
    except (KeyError, TypeError):
        return None


def _get_qualifier_year(claim: dict, qual_prop: str) -> Optional[int]:
    """Extract year from a qualifier on a claim (e.g. P582 on a P31 claim)."""
    try:
        quals = claim.get("qualifiers", {}).get(qual_prop, [])
        for q in quals:
            tv = q.get("datavalue", {}).get("value", {}).get("time")
            yr = _extract_wikidata_year(tv)
            if yr is not None:
                return yr
    except (KeyError, TypeError):
        pass
    return None


# ========== SHARED STATE ==========

_shutdown_flag: Optional[Value] = None
_shared_bytes:  Optional[Array] = None
_shared_scanned: Optional[Array] = None
_shared_gpes:   Optional[Array] = None
_shared_errors: Optional[Array] = None
_shared_skipped_large: Optional[Array] = None
_shared_filtered: Optional[Array] = None


def _init_worker(shutdown_flag, s_bytes, s_scanned, s_gpes, s_errors, s_skip, s_filtered):
    global _shutdown_flag, _shared_bytes, _shared_scanned
    global _shared_gpes, _shared_errors, _shared_skipped_large, _shared_filtered
    _shutdown_flag = shutdown_flag
    _shared_bytes = s_bytes
    _shared_scanned = s_scanned
    _shared_gpes = s_gpes
    _shared_errors = s_errors
    _shared_skipped_large = s_skip
    _shared_filtered = s_filtered
    signal.signal(signal.SIGINT, signal.SIG_IGN)


# ========== ENTITY PARSING + FILTERING ==========

def _check_gpe_and_filter(entity: dict) -> tuple[Optional[dict], Optional[str]]:
    """
    Check if entity is a GPE, then apply temporal/quality filters.

    Returns:
        (data_dict, None)       — entity passes all filters
        (None, reason_string)   — entity is a GPE but was filtered out
        (None, None)            — entity is not a GPE at all
    """
    if entity.get("type") != "item":
        return None, None

    qid = entity.get("id", "")
    claims = entity.get("claims")
    if not claims:
        return None, None

    # ---- Check P31 (instance of) for GPE types ----
    p31 = claims.get("P31")
    if not p31:
        return None, None

    is_gpe = False
    is_subnational = False
    p31_end_year = None       # latest P582 qualifier on any P31=country claim

    for claim in p31[:20]:
        try:
            val_id = claim["mainsnak"]["datavalue"]["value"]["id"]
        except (KeyError, TypeError):
            continue

        if val_id in GPE_TYPES:
            is_gpe = True
            # Check for P582 (end time) qualifier on this P31 claim
            ey = _get_qualifier_year(claim, "P582")
            if ey is not None:
                p31_end_year = max(p31_end_year or 0, ey)

        if val_id in SUBNATIONAL_TYPES:
            is_subnational = True

    if not is_gpe:
        return None, None

    # ---- Extract basic data (needed for filter logging even if rejected) ----
    label = (entity.get("labels") or {}).get("en", {}).get("value", "")
    gloss = (entity.get("descriptions") or {}).get("en", {}).get("value", "")

    # ---- FILTER: Blocklist ----
    if qid in BLOCKLIST_QIDS:
        return None, f"BLOCKLIST  {qid}  {label}"

    # ---- FILTER: No English label ----
    if not label or label.startswith("Unknown_"):
        return None, f"NO_LABEL  {qid}"

    # ---- FILTER: Subnational ----
    if is_subnational:
        return None, f"SUBNATIONAL  {qid}  {label}"

    # ---- FILTER: Temporal — P576 (dissolved/abolished date) ----
    dissolved_year = None
    p576 = claims.get("P576")
    if p576:
        for claim in p576[:5]:
            yr = _get_claim_year(claim)
            if yr is not None:
                dissolved_year = max(dissolved_year or 0, yr)

    # Use the latest of P576 and P31-end-time as the "end year"
    end_year = None
    if dissolved_year is not None and p31_end_year is not None:
        end_year = max(dissolved_year, p31_end_year)
    elif dissolved_year is not None:
        end_year = dissolved_year
    elif p31_end_year is not None:
        end_year = p31_end_year

    if end_year is not None and end_year < YEAR_CUTOFF:
        return None, f"PRE_{YEAR_CUTOFF}_P576  {qid}  {label}  end={end_year}"

    # ---- FILTER: Temporal — gloss-based date heuristic ----
    # Only apply if no structured end_year was found (fallback)
    if end_year is None and gloss:
        years = [int(y) for y in _YEAR_RE.findall(gloss) if 100 < int(y) < 2100]
        if years:
            max_gloss_year = max(years)
            if max_gloss_year < YEAR_CUTOFF:
                return None, (
                    f"PRE_{YEAR_CUTOFF}_GLOSS  {qid}  {label}  "
                    f"gloss_max_year={max_gloss_year}  \"{gloss[:100]}\""
                )

    # ---- PASSED all filters — build result ----
    continent = None
    p30 = claims.get("P30")
    if p30:
        try:
            continent = CONTINENTS.get(
                p30[0]["mainsnak"]["datavalue"]["value"]["id"]
            )
        except (KeyError, TypeError, IndexError):
            pass

    names = {label}
    aliases = (entity.get("aliases") or {}).get("en")
    if aliases:
        for a in aliases:
            v = a.get("value")
            if v:
                names.add(v)

    # Inception year (informational, not a filter)
    inception = None
    p571 = claims.get("P571")
    if p571:
        for claim in p571[:3]:
            yr = _get_claim_year(claim)
            if yr is not None:
                inception = yr
                break

    return {
        "qid":           qid,
        "primary_label": label,
        "gloss":         gloss,
        "continent":     continent,
        "names":         list(names),
        "inception":     inception,
        "dissolved":     end_year,
    }, None


# ========== PARALLEL WORKER ==========

def _worker(args: tuple) -> dict:
    range_start, range_end, worker_id, is_test, test_limit = args
    idx = worker_id

    results: list[dict] = []
    local_scanned = 0
    local_errors  = 0
    local_gpes    = 0
    local_bytes   = 0
    local_skip_lg = 0
    local_filtered = 0

    FLUSH_INTERVAL = 5000
    lines_since_flush = 0
    error_lines: list[str] = []
    filter_lines: list[str] = []

    with open(DUMP_PATH, "rb", buffering=READ_BUFFER) as f:
        f.seek(range_start)

        while True:
            if _shutdown_flag and _shutdown_flag.value:
                break

            pos_before = f.tell()
            if pos_before >= range_end:
                break

            raw = f.readline()
            if not raw:
                break

            line_len = len(raw)
            local_bytes += line_len

            if line_len > MAX_LINE_BYTES:
                local_skip_lg += 1
                error_lines.append(
                    f"OVERSIZE  worker={idx}  offset={pos_before}  "
                    f"size={fmt_bytes(line_len)}  head={raw[:120]!r}\n"
                )
                continue

            line = raw.strip()
            if line in (b"[", b"]", b""):
                continue
            if line.endswith(b","):
                line = line[:-1]

            try:
                entity = orjson.loads(line)
            except Exception as exc:
                local_errors += 1
                error_lines.append(
                    f"JSON_ERR  worker={idx}  offset={pos_before}  "
                    f"exc={type(exc).__name__}: {exc}  head={raw[:200]!r}\n"
                )
                continue

            local_scanned += 1

            try:
                data, filter_reason = _check_gpe_and_filter(entity)
                if data:
                    results.append(data)
                    local_gpes += 1
                elif filter_reason:
                    local_filtered += 1
                    filter_lines.append(f"{filter_reason}\n")
            except Exception as exc:
                local_errors += 1
                eid = entity.get("id", "?")
                error_lines.append(
                    f"EXTRACT   worker={idx}  offset={pos_before}  "
                    f"entity={eid}  exc={type(exc).__name__}: {exc}\n"
                )

            if is_test and local_gpes >= test_limit:
                break

            lines_since_flush += 1
            if lines_since_flush >= FLUSH_INTERVAL:
                _shared_bytes[idx]         = local_bytes
                _shared_scanned[idx]       = local_scanned
                _shared_gpes[idx]          = local_gpes
                _shared_errors[idx]        = local_errors
                _shared_skipped_large[idx] = local_skip_lg
                _shared_filtered[idx]      = local_filtered
                lines_since_flush = 0

                if error_lines:
                    try:
                        with open(ERROR_LOG_PATH, "a") as ef:
                            ef.writelines(error_lines)
                    except OSError:
                        pass
                    error_lines.clear()
                if filter_lines:
                    try:
                        with open(FILTER_LOG_PATH, "a") as ff:
                            ff.writelines(filter_lines)
                    except OSError:
                        pass
                    filter_lines.clear()

    # Final flush
    _shared_bytes[idx]         = local_bytes
    _shared_scanned[idx]       = local_scanned
    _shared_gpes[idx]          = local_gpes
    _shared_errors[idx]        = local_errors
    _shared_skipped_large[idx] = local_skip_lg
    _shared_filtered[idx]      = local_filtered

    for path, lines in [(ERROR_LOG_PATH, error_lines), (FILTER_LOG_PATH, filter_lines)]:
        if lines:
            try:
                with open(path, "a") as fh:
                    fh.writelines(lines)
            except OSError:
                pass

    return {
        "worker_id":      worker_id,
        "results":        results,
        "scanned":        local_scanned,
        "errors":         local_errors,
        "gpes":           local_gpes,
        "filtered":       local_filtered,
        "bytes_read":     local_bytes,
        "skipped_large":  local_skip_lg,
        "range_start":    range_start,
        "range_end":      range_end,
    }


# ========== FILE SPLITTING ==========

def _compute_ranges(file_size: int, n_workers: int) -> list[tuple[int, int]]:
    chunk = file_size // n_workers
    ranges: list[tuple[int, int]] = []
    with open(DUMP_PATH, "rb") as f:
        start = 0
        for i in range(n_workers):
            if i == n_workers - 1:
                ranges.append((start, file_size))
            else:
                end = start + chunk
                f.seek(end)
                f.readline()
                end = f.tell()
                ranges.append((start, end))
                start = end
    return ranges


# ========== CHECKPOINT ==========

def _save_checkpoint(completed_ranges, results):
    data = {
        "dump_path":  DUMP_PATH,
        "dump_size":  os.path.getsize(DUMP_PATH),
        "completed":  completed_ranges,
        "gpe_count":  len(results),
        "gpes":       results,
        "timestamp":  time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    tmp = CHECKPOINT_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, CHECKPOINT_PATH)


def _load_checkpoint() -> Optional[dict]:
    if not os.path.exists(CHECKPOINT_PATH):
        return None
    try:
        with open(CHECKPOINT_PATH) as f:
            data = json.load(f)
        if data.get("dump_path") != DUMP_PATH:
            return None
        if data.get("dump_size") != os.path.getsize(DUMP_PATH):
            print("   ⚠️  Checkpoint dump_size differs — ignoring checkpoint.\n")
            return None
        return data
    except Exception:
        return None


# ========== PROGRESS DASHBOARD ==========

def _print_dashboard(n_workers, ranges, file_size, t0, finished):
    now = time.time()
    elapsed = now - t0

    total_bytes    = sum(_shared_bytes[i] for i in range(n_workers))
    total_scanned  = sum(_shared_scanned[i] for i in range(n_workers))
    total_gpes     = sum(_shared_gpes[i] for i in range(n_workers))
    total_errors   = sum(_shared_errors[i] for i in range(n_workers))
    total_skip     = sum(_shared_skipped_large[i] for i in range(n_workers))
    total_filtered = sum(_shared_filtered[i] for i in range(n_workers))

    pct  = total_bytes / file_size * 100 if file_size else 0
    rate = total_bytes / elapsed if elapsed > 0 else 0
    eta  = (file_size - total_bytes) / rate if rate > 0 else 0

    lines = []
    lines.append("─" * 74)
    lines.append(
        f"  ⏱  {format_eta(elapsed)} elapsed  |  "
        f"{pct:5.1f}%  |  "
        f"{fmt_bytes(rate)}/s  |  "
        f"ETA {format_eta(eta)}"
    )
    lines.append(
        f"  📊 Scanned: {total_scanned:>14,}  |  "
        f"GPEs: {total_gpes:<5}  |  "
        f"Filtered: {total_filtered:<5}  |  "
        f"Errors: {total_errors:<7,}"
    )
    lines.append(
        f"  💾 Read: {fmt_bytes(total_bytes):>10s} / {fmt_bytes(file_size)}  |  "
        f"Oversize skips: {total_skip}"
    )

    worker_parts = []
    for i in range(n_workers):
        rng_size = ranges[i][1] - ranges[i][0]
        w_pct = _shared_bytes[i] / rng_size * 100 if rng_size else 100
        status = "✓" if i in finished else "▶"
        worker_parts.append(f"{status}W{i}:{w_pct:4.0f}%")
    for row_start in range(0, len(worker_parts), 8):
        lines.append("  " + "  ".join(worker_parts[row_start:row_start + 8]))

    lines.append("─" * 74)
    sys.stderr.write("\n".join(lines) + "\n")
    sys.stderr.flush()


# ========== ORCHESTRATOR ==========

def stream_and_process() -> list[dict]:
    file_size = os.path.getsize(DUMP_PATH)
    n_workers = MAX_WORKERS
    ranges    = _compute_ranges(file_size, n_workers)

    # Checkpoint resume
    ckpt = _load_checkpoint()
    prior_results: list[dict] = []
    completed_set: set[tuple[int, int]] = set()

    if ckpt:
        completed_set = {tuple(r) for r in ckpt["completed"]}
        prior_results = ckpt.get("gpes", [])
        remaining_work = [
            (i, r) for i, r in enumerate(ranges) if r not in completed_set
        ]
        if remaining_work:
            print(
                f"📋 Checkpoint found ({ckpt['timestamp']})  —  "
                f"{len(ranges) - len(remaining_work)}/{len(ranges)} ranges done, "
                f"{ckpt['gpe_count']} GPEs so far."
            )
            print(f"   Resuming {len(remaining_work)} remaining range(s).\n")
        else:
            print("📋 All ranges complete in checkpoint. Re-running from scratch.\n")
            remaining_work = list(enumerate(ranges))
            completed_set = set()
            prior_results = []
    else:
        remaining_work = list(enumerate(ranges))

    print(f"📂 File:       {DUMP_PATH}")
    print(f"   Size:       {fmt_bytes(file_size)}")
    print(f"   Workers:    {n_workers}  |  Ranges: {len(ranges)}")
    print(f"   Year cutoff: {YEAR_CUTOFF}+")
    print(f"   Blocklist:  {len(BLOCKLIST_QIDS)} QIDs")
    print(f"   Error log:  {ERROR_LOG_PATH}")
    print(f"   Filter log: {FILTER_LOG_PATH}")
    print()

    for i, (s, e) in enumerate(ranges):
        marker = "⏭ " if (s, e) in completed_set else "▶ "
        print(f"   {marker}Range {i:>2d}:  {fmt_bytes(s):>10s} — {fmt_bytes(e):>10s}  ({fmt_bytes(e - s)})")
    print()

    if not remaining_work:
        print("   Nothing to do.")
        return prior_results

    # Shared memory
    shutdown_flag = Value(ctypes.c_int, 0)
    s_bytes    = Array(ctypes.c_longlong, n_workers)
    s_scanned  = Array(ctypes.c_longlong, n_workers)
    s_gpes     = Array(ctypes.c_longlong, n_workers)
    s_errors   = Array(ctypes.c_longlong, n_workers)
    s_skip     = Array(ctypes.c_longlong, n_workers)
    s_filtered = Array(ctypes.c_longlong, n_workers)

    for i, (rs, re_) in enumerate(ranges):
        if (rs, re_) in completed_set:
            s_bytes[i] = re_ - rs

    global _shutdown_flag, _shared_bytes, _shared_scanned
    global _shared_gpes, _shared_errors, _shared_skipped_large, _shared_filtered
    _shutdown_flag = shutdown_flag
    _shared_bytes = s_bytes
    _shared_scanned = s_scanned
    _shared_gpes = s_gpes
    _shared_errors = s_errors
    _shared_skipped_large = s_skip
    _shared_filtered = s_filtered

    # SIGINT handler
    interrupted = False

    def _sigint_handler(signum, frame):
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            shutdown_flag.value = 1
            sys.stderr.write(
                "\n\n  🛑  Ctrl-C received — workers stopping after current line…\n"
                "      (press Ctrl-C again to force-kill)\n\n"
            )
        else:
            sys.stderr.write("\n  ⚡ Force kill.\n")
            sys.exit(1)

    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, _sigint_handler)

    work = [
        (ranges[i][0], ranges[i][1], i, IS_TEST_RUN, TEST_LIMIT)
        for i, _ in remaining_work
    ]

    all_results: list[dict] = list(prior_results)
    finished: set[int] = {i for i, r in enumerate(ranges) if r in completed_set}
    completed_ranges = list(completed_set)
    t0 = time.time()
    last_dashboard = t0

    try:
        with Pool(
            min(n_workers, len(work)),
            initializer=_init_worker,
            initargs=(shutdown_flag, s_bytes, s_scanned, s_gpes, s_errors, s_skip, s_filtered),
        ) as pool:
            async_results = [pool.apply_async(_worker, (w,)) for w in work]

            while True:
                all_done = all(ar.ready() for ar in async_results)
                now = time.time()
                if now - last_dashboard >= PROGRESS_SEC:
                    _print_dashboard(n_workers, ranges, file_size, t0, finished)
                    last_dashboard = now
                if all_done:
                    break
                time.sleep(0.5)

            for ar in async_results:
                try:
                    result = ar.get(timeout=60)
                    wid = result["worker_id"]
                    finished.add(wid)
                    all_results.extend(result["results"])
                    completed_ranges.append(
                        (result["range_start"], result["range_end"])
                    )
                    elapsed = time.time() - t0
                    print(
                        f"  ✓ Worker {wid:>2d} complete  |  "
                        f"GPEs: {result['gpes']:<4d}  |  "
                        f"Filtered: {result['filtered']:<4d}  |  "
                        f"Scanned: {result['scanned']:>13,}  |  "
                        f"Errors: {result['errors']:<7,}  |  "
                        f"Read: {fmt_bytes(result['bytes_read'])}  |  "
                        f"Time: {format_eta(elapsed)}"
                    )
                except Exception as exc:
                    print(f"  ✗ Worker error: {type(exc).__name__}: {exc}")

    finally:
        signal.signal(signal.SIGINT, original_sigint)

    _print_dashboard(n_workers, ranges, file_size, t0, finished)
    _save_checkpoint(completed_ranges, all_results)

    elapsed = time.time() - t0
    total_scanned  = sum(s_scanned[i] for i in range(n_workers))
    total_errors   = sum(s_errors[i] for i in range(n_workers))
    total_skip     = sum(s_skip[i] for i in range(n_workers))
    total_filtered = sum(s_filtered[i] for i in range(n_workers))

    print(
        f"\n{'🛑 Interrupted' if interrupted else '✅ Complete'}  |  "
        f"GPEs: {len(all_results)}  |  "
        f"Filtered: {total_filtered}  |  "
        f"Scanned: {total_scanned:,}  |  "
        f"Errors: {total_errors:,}  |  "
        f"Time: {format_eta(elapsed)}"
    )
    if total_errors > 0 or total_skip > 0:
        print(f"   📄 Error details: {ERROR_LOG_PATH}")
    if total_filtered > 0:
        print(f"   📄 Filter log (audit what was dropped): {FILTER_LOG_PATH}")
    if interrupted:
        print(f"   📋 Checkpoint saved: {CHECKPOINT_PATH}")
        print(f"      Re-run to resume.")
    else:
        if os.path.exists(CHECKPOINT_PATH):
            os.remove(CHECKPOINT_PATH)
            print(f"   🧹 Checkpoint cleaned up.")

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

:Geopolitical_Entity rdfs:subClassOf :Entity .
:Geopolitical_Entry rdfs:subClassOf :Entry .

:part_of a rdfs:Property, :relation .
:sense a rdfs:Property .
:pos a rdfs:Property .
:gloss a rdfs:Property .
:identifier a rdfs:Property .
:source a rdfs:Property .

# Part of Speech Entities
:Noun.entity.01 a :Entity ;
    rdfs:label "Noun"@en .

# ==========================================
# GEOPOLITICAL ENTITIES
# ==========================================

:Earth.entity.01 a :Geopolitical_Entity ;
    rdfs:label "Earth (entity)"@en ;
    :gloss "third planet from the Sun in the Solar System"@en ;
    :identifier wiki:Q2 ;
    :source <https://www.wikidata.org/> .

:Africa.entity.01 a :Geopolitical_Entity ;
    rdfs:label "Africa (entity)"@en ;
    :gloss "continent on the Earth's northern and southern hemispheres"@en ;
    :part_of :Earth.entity.01 ;
    :identifier wiki:Q15 ;
    :source <https://www.wikidata.org/> .

:Antarctica.entity.01 a :Geopolitical_Entity ;
    rdfs:label "Antarctica (entity)"@en ;
    :gloss "continent located at the South Pole"@en ;
    :part_of :Earth.entity.01 ;
    :identifier wiki:Q51 ;
    :source <https://www.wikidata.org/> .

:Asia.entity.01 a :Geopolitical_Entity ;
    rdfs:label "Asia (entity)"@en ;
    :gloss "continent on the Earth's northern and eastern hemispheres"@en ;
    :part_of :Earth.entity.01 ;
    :identifier wiki:Q48 ;
    :source <https://www.wikidata.org/> .

:Europe.entity.01 a :Geopolitical_Entity ;
    rdfs:label "Europe (entity)"@en ;
    :gloss "continent on the Earth's northwestern quadrant"@en ;
    :part_of :Earth.entity.01 ;
    :identifier wiki:Q46 ;
    :source <https://www.wikidata.org/> .

:North_America.entity.01 a :Geopolitical_Entity ;
    rdfs:label "North America (entity)"@en ;
    :gloss "continent on the Earth's northwestern quadrant"@en ;
    :part_of :Earth.entity.01 ;
    :identifier wiki:Q49 ;
    :source <https://www.wikidata.org/> .

:Oceania.entity.01 a :Geopolitical_Entity ;
    rdfs:label "Oceania (entity)"@en ;
    :gloss "geographical region and continent consisting of many islands"@en ;
    :part_of :Earth.entity.01 ;
    :identifier wiki:Q538 ;
    :source <https://www.wikidata.org/> .

:South_America.entity.01 a :Geopolitical_Entity ;
    rdfs:label "South America (entity)"@en ;
    :gloss "continent mostly in the Southern Hemisphere"@en ;
    :part_of :Earth.entity.01 ;
    :identifier wiki:Q18 ;
    :source <https://www.wikidata.org/> .

"""

ENTRY_HEADER = """\
@prefix :      <https://falcontologist.github.io/shacl-demo/ontology/> .
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .

# ==========================================
# GEOPOLITICAL ENTRIES
# ==========================================

:Earth.entry.01 a :Geopolitical_Entry ;
    rdfs:label "Earth"@en ;
    :sense :Earth.entity.01 ;
    :pos :Noun.entity.01 ;
    :source <https://www.wikidata.org/> .

:Africa.entry.01 a :Geopolitical_Entry ;
    rdfs:label "Africa"@en ;
    :sense :Africa.entity.01 ;
    :pos :Noun.entity.01 ;
    :source <https://www.wikidata.org/> .

:Antarctica.entry.01 a :Geopolitical_Entry ;
    rdfs:label "Antarctica"@en ;
    :sense :Antarctica.entity.01 ;
    :pos :Noun.entity.01 ;
    :source <https://www.wikidata.org/> .

:Asia.entry.01 a :Geopolitical_Entry ;
    rdfs:label "Asia"@en ;
    :sense :Asia.entity.01 ;
    :pos :Noun.entity.01 ;
    :source <https://www.wikidata.org/> .

:Europe.entry.01 a :Geopolitical_Entry ;
    rdfs:label "Europe"@en ;
    :sense :Europe.entity.01 ;
    :pos :Noun.entity.01 ;
    :source <https://www.wikidata.org/> .

:North_America.entry.01 a :Geopolitical_Entry ;
    rdfs:label "North America"@en ;
    :sense :North_America.entity.01 ;
    :pos :Noun.entity.01 ;
    :source <https://www.wikidata.org/> .

:Oceania.entry.01 a :Geopolitical_Entry ;
    rdfs:label "Oceania"@en ;
    :sense :Oceania.entity.01 ;
    :pos :Noun.entity.01 ;
    :source <https://www.wikidata.org/> .

:South_America.entry.01 a :Geopolitical_Entry ;
    rdfs:label "South America"@en ;
    :sense :South_America.entity.01 ;
    :pos :Noun.entity.01 ;
    :source <https://www.wikidata.org/> .

"""


def generate_ttls(records: list[dict]) -> None:
    entity_file = os.path.join(OUTPUT_DIR, "gpe.entity.ttl")
    entry_file  = os.path.join(OUTPUT_DIR, "gpe_entry.ttl")

    # De-duplicate by qid
    seen_qids: dict[str, dict] = {}
    for r in records:
        seen_qids.setdefault(r["qid"], r)
    unique = sorted(seen_qids.values(), key=lambda r: r["primary_label"])

    # --- Entity file ---
    parts: list[str] = [ENTITY_HEADER]
    for r in unique:
        safe = sanitize_uri(r["primary_label"])
        block = f":{safe}.entity.01 a :Geopolitical_Entity ;\n"
        block += f'    rdfs:label "{escape_turtle(r["primary_label"])} (entity)"@en ;\n'
        if r["gloss"]:
            block += f'    :gloss "{escape_turtle(r["gloss"])}"@en ;\n'
        if r["continent"]:
            block += f'    :part_of :{r["continent"]}.entity.01 ;\n'
        block += f'    :identifier wiki:{r["qid"]} ;\n'
        block += f'    :source <https://www.wikidata.org/> .\n\n'
        parts.append(block)

    with open(entity_file, "w", encoding="utf-8") as f:
        f.write("".join(parts))
    print(f"✅ Wrote {entity_file}  ({len(unique)} entities)")

    # --- Entry file ---
    qid_to_label = {r["qid"]: r["primary_label"] for r in unique}

    name_to_qids: dict[str, set[str]] = {}
    for r in unique:
        for name in r["names"]:
            name_to_qids.setdefault(name, set()).add(r["qid"])

    parts = [ENTRY_HEADER]
    counter = 1
    for name in sorted(name_to_qids):
        safe = sanitize_uri(name)
        if not safe:
            continue
        sense_uris = []
        for q in sorted(name_to_qids[name]):
            lbl = qid_to_label.get(q)
            if lbl:
                sense_uris.append(f":{sanitize_uri(lbl)}.entity.01")
        if not sense_uris:
            continue
        block = f":{safe}.entry.{counter:02d} a :Geopolitical_Entry ;\n"
        block += f'    rdfs:label "{escape_turtle(name)}"@en ;\n'
        block += f'    :sense {", ".join(sense_uris)} ;\n'
        block += f'    :pos :Noun.entity.01 ;\n'
        block += f'    :source <https://www.wikidata.org/> .\n\n'
        parts.append(block)
        counter += 1

    with open(entry_file, "w", encoding="utf-8") as f:
        f.write("".join(parts))
    print(f"✅ Wrote {entry_file}  ({counter - 1} entries)")


# ========== MAIN ==========

def main() -> None:
    print()
    print("=" * 64)
    print("  Wikidata GPE Extractor  (v4 — 1850-present, filtered)")
    print("=" * 64)
    print()

    if not os.path.exists(DUMP_PATH):
        print(f"❌ Dump not found at {DUMP_PATH}")
        sys.exit(1)

    records = stream_and_process()
    if records:
        generate_ttls(records)
    else:
        print("⚠️  No GPE entities found.")


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    main()