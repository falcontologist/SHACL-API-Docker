#!/usr/bin/env python3
"""
Product TTL Cleaner
===================
Reads product_entity.ttl and product_entry.ttl, removes false positive
entities based on gloss heuristics and missing-label rules, then cascades
removals into the entry file (removing dead sense links, dropping entries
with no remaining senses).

Usage:
    python product_cleaner.py

Reads from / writes to the configured directory. Originals are backed up
as .bak files. A removal log is written for audit.
"""

import os
import re
import sys
import time

# ========== CONFIGURATION ==========

BASE_DIR     = "/Volumes/Extreme Pro/FKG/SHACL-API-Docker"
ENTITY_FILE  = os.path.join(BASE_DIR, "product_entity.ttl")
ENTRY_FILE   = os.path.join(BASE_DIR, "product_entry.ttl")
REMOVAL_LOG  = os.path.join(BASE_DIR, "logs", f"product_removed_{time.strftime('%Y%m%d_%H%M%S')}.log")

os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)

# ========== FALSE POSITIVE GLOSS PATTERNS ==========
# Each pattern is (regex, category_label). Checked case-insensitively
# against the :gloss value of each entity.

GLOSS_REJECT_PATTERNS: list[tuple[str, str]] = [
    # --- Military vessels (biggest source of contamination) ---
    (r"submarine", "SUBMARINE"),
    (r"destroyer", "MILITARY_VESSEL"),
    (r"battleship", "MILITARY_VESSEL"),
    (r"\bcruiser\b", "MILITARY_VESSEL"),
    (r"frigate", "MILITARY_VESSEL"),
    (r"corvette", "MILITARY_VESSEL"),
    (r"warship", "MILITARY_VESSEL"),
    (r"torpedo boat", "MILITARY_VESSEL"),
    (r"Type VII", "SUBMARINE"),
    (r"Type IX", "SUBMARINE"),
    (r"Type XXI", "SUBMARINE"),
    (r"Type XXIII", "SUBMARINE"),
    (r"Type II[ABCD]?\b", "SUBMARINE"),
    (r"U-boat", "SUBMARINE"),
    (r"minesweeper", "MILITARY_VESSEL"),
    (r"gunboat", "MILITARY_VESSEL"),

    # --- Ships / boats (non-product vessels) ---
    (r"\bferry\b", "VESSEL"),
    (r"\btanker\b", "VESSEL"),
    (r"\bbarge\b", "VESSEL"),
    (r"\btrawler\b", "VESSEL"),
    (r"\byacht\b", "VESSEL"),
    (r"cargo ship", "VESSEL"),
    (r"container ship", "VESSEL"),
    (r"icebreaker", "VESSEL"),
    (r"sailing vessel", "VESSEL"),
    (r"ocean liner", "VESSEL"),
    (r"training ship", "VESSEL"),
    (r"patrol boat", "VESSEL"),

    # --- Aircraft (specific military/transport types, not consumer products) ---
    (r"aircraft family", "AIRCRAFT"),
    (r"airliner family", "AIRCRAFT"),
    (r"bomber family", "AIRCRAFT"),
    (r"bomber aircraft", "AIRCRAFT"),
    (r"fighter family", "AIRCRAFT"),
    (r"fighter aircraft", "AIRCRAFT"),
    (r"flying boat", "AIRCRAFT"),
    (r"reconnaissance aircraft", "AIRCRAFT"),
    (r"trainer aircraft", "AIRCRAFT"),
    (r"utility aircraft", "AIRCRAFT"),
    (r"transport aircraft", "AIRCRAFT"),
    (r"attack helicopter family", "AIRCRAFT"),
    (r"airlifter", "AIRCRAFT"),
    (r"prototype aircraft", "AIRCRAFT"),
    (r"military helicopter", "AIRCRAFT"),
    (r"jet airliner family", "AIRCRAFT"),

    # --- Biology / taxonomy ---
    (r"species of\b", "BIOLOGY"),
    (r"genus of\b", "BIOLOGY"),
    (r"family of plants", "BIOLOGY"),
    (r"\bbreed of\b", "BIOLOGY"),
    (r"\bcultivar\b", "BIOLOGY"),
    (r"\btaxon\b", "BIOLOGY"),
    (r"\bvarietal\b", "BIOLOGY"),
    (r"species in the", "BIOLOGY"),
    (r"genus in the", "BIOLOGY"),

    # --- Chemical compounds ---
    (r"chemical compound", "CHEMICAL"),
    (r"chemical element", "CHEMICAL"),
    (r"organic compound", "CHEMICAL"),
    (r"inorganic compound", "CHEMICAL"),
    (r"\bmineral\b", "CHEMICAL"),

    # --- Media content (not products in this context) ---
    (r"television series", "MEDIA"),
    (r"\bTV series\b", "MEDIA"),
    (r"\bTV show\b", "MEDIA"),
    (r"\bmanga\b", "MEDIA"),
    (r"\banime\b", "MEDIA"),
    (r"(?<!visual )\bnovel\b", "MEDIA"),
    (r"\balbum\b", "MEDIA"),
    (r"\bnewspaper\b", "MEDIA"),
    (r"\bmagazine\b", "MEDIA"),
    (r"\bjournal\b", "MEDIA"),
    (r"\bradio\b", "MEDIA"),
    (r"comic book", "MEDIA"),
    (r"comic strip", "MEDIA"),
    (r"\d{4}.*\bfilm\b", "MEDIA"),
    (r"^film\b", "MEDIA"),
    (r"\bdocumentary film\b", "MEDIA"),
    (r"\bshort film\b", "MEDIA"),
    (r"\bsilent film\b", "MEDIA"),
    (r"\banimated film\b", "MEDIA"),
    (r"film directed", "MEDIA"),
    (r"\bmovie\b", "MEDIA"),
    (r"web series", "MEDIA"),
    (r"podcast", "MEDIA"),

    # --- Organizations / companies ---
    (r"\bcompany\b", "ORGANIZATION"),
    (r"\benterprise\b", "ORGANIZATION"),
    (r"\bcorporation\b", "ORGANIZATION"),
    (r"\borganization\b", "ORGANIZATION"),
    (r"\borganisation\b", "ORGANIZATION"),
    (r"\btrade union\b", "ORGANIZATION"),
    (r"\bcooperative\b", "ORGANIZATION"),
    (r"\bassociation\b", "ORGANIZATION"),
    (r"\bairline\b", "ORGANIZATION"),
    (r"\bbank\b", "ORGANIZATION"),
    (r"stock exchange", "ORGANIZATION"),

    # --- Events / competitions ---
    (r"\bevent\b", "EVENT"),
    (r"\bfestival\b", "EVENT"),
    (r"\bchampionship\b", "EVENT"),
    (r"\btournament\b", "EVENT"),
    (r"\bcompetition\b", "EVENT"),
    (r"\belection\b", "EVENT"),
    (r"\bcampaign\b", "EVENT"),
    (r"\bconference\b", "EVENT"),
    (r"\bgrand prix\b", "EVENT"),
    (r"\bendurance\b", "EVENT"),
    (r"\brace series\b", "EVENT"),
    (r"\brally\b", "EVENT"),

    # --- Geography / infrastructure ---
    (r"\breservoir\b", "GEOGRAPHY"),
    (r"\blake\b", "GEOGRAPHY"),
    (r"\briver\b", "GEOGRAPHY"),
    (r"\bdam\b", "GEOGRAPHY"),
    (r"\bmountain\b", "GEOGRAPHY"),
    (r"\bisland\b", "GEOGRAPHY"),
    (r"\bprovince\b", "GEOGRAPHY"),
    (r"\bdistrict\b", "GEOGRAPHY"),
    (r"\bmunicipality\b", "GEOGRAPHY"),
    (r"\bvillage\b", "GEOGRAPHY"),
    (r"\bairport\b", "GEOGRAPHY"),
    (r"\brailway\b", "GEOGRAPHY"),
    (r"power plant", "GEOGRAPHY"),
    (r"nuclear plant", "GEOGRAPHY"),
    (r"\bcanal\b", "GEOGRAPHY"),
    (r"\bbridge\b", "GEOGRAPHY"),

    # --- Abstract concepts / services / categories ---
    (r"\btype of\b", "ABSTRACT"),
    (r"\bclass of\b", "ABSTRACT"),
    (r"\bgenre\b", "ABSTRACT"),
    (r"\bform of\b", "ABSTRACT"),
    (r"\bbranch of\b", "ABSTRACT"),
    (r"\bfield of\b", "ABSTRACT"),
    (r"economic activity", "ABSTRACT"),
    (r"\bindustry\b", "ABSTRACT"),
    (r"\bsector\b", "ABSTRACT"),
    (r"\bprofession\b", "ABSTRACT"),
    (r"\boccupation\b", "ABSTRACT"),
    (r"\bdiscipline\b", "ABSTRACT"),
    (r"\bscience of\b", "ABSTRACT"),
    (r"\bstyle of\b", "ABSTRACT"),
    (r"\bpractice\b", "ABSTRACT"),
    (r"\btechnique\b", "ABSTRACT"),

    # --- People / characters ---
    (r"fictional character", "PEOPLE"),
    (r"^person who", "PEOPLE"),
    (r"^a person", "PEOPLE"),

    # --- Wikimedia junk ---
    (r"Wikimedia", "WIKIMEDIA"),
    (r"list article", "WIKIMEDIA"),

    # --- Awards / certifications ---
    (r"\baward\b", "AWARD"),
    (r"\bprize\b", "AWARD"),
    (r"\bcertification\b", "AWARD"),
    (r"\bmedal\b", "AWARD"),

    # --- Military (non-vessel) ---
    (r"\bmissile\b", "MILITARY"),
    (r"\bammunition\b", "MILITARY"),
    (r"\bweapon system\b", "MILITARY"),
    (r"\bwarhead\b", "MILITARY"),
    (r"anti-ship missile", "MILITARY"),
    (r"anti-aircraft", "MILITARY"),
    (r"armoured? personnel carrier", "MILITARY"),
    (r"\btank\b.*\bmilitary\b", "MILITARY"),
]

# Compile once
_GLOSS_REJECT_COMPILED = [
    (re.compile(pat, re.IGNORECASE), cat) for pat, cat in GLOSS_REJECT_PATTERNS
]

# ========== PARSER ==========

def parse_entity_blocks(filepath: str) -> tuple[str, list[dict]]:
    """Parse the entity TTL file into header + list of block dicts."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Split at entity declarations
    # Header = everything before the first entity block
    parts = re.split(r"\n(?=:[A-Za-z0-9_]+\.product_entity\.\d+ a :Product_Entity)", content)
    header = parts[0]
    blocks = []
    for raw_block in parts[1:]:
        raw_block = raw_block.strip()
        if not raw_block:
            continue
        # Extract URI
        uri_match = re.match(r"(:\S+\.product_entity\.\d+)", raw_block)
        # Extract gloss
        gloss_match = re.search(r':gloss "(.+?)"@en', raw_block)
        # Extract label
        label_match = re.search(r'rdfs:label "(.+?)"@en', raw_block)
        # Extract QID
        qid_match = re.search(r':identifier wiki:(Q\d+)', raw_block)

        blocks.append({
            "uri": uri_match.group(1) if uri_match else "",
            "gloss": gloss_match.group(1) if gloss_match else "",
            "label": label_match.group(1) if label_match else "",
            "qid": qid_match.group(1) if qid_match else "",
            "raw": raw_block,
        })
    return header, blocks


def parse_entry_blocks(filepath: str) -> tuple[str, list[dict]]:
    """Parse the entry TTL file into header + list of block dicts."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    parts = re.split(r"\n(?=:[A-Za-z0-9_]+\.product_entry a :Product_Entry)", content)
    header = parts[0]
    blocks = []
    for raw_block in parts[1:]:
        raw_block = raw_block.strip()
        if not raw_block:
            continue
        # Extract URI
        uri_match = re.match(r"(:\S+\.product_entry)", raw_block)
        # Extract sense URIs
        sense_match = re.search(r":sense (.+?) ;", raw_block)
        sense_uris = []
        if sense_match:
            sense_uris = [s.strip() for s in sense_match.group(1).split(",")]

        blocks.append({
            "uri": uri_match.group(1) if uri_match else "",
            "sense_uris": sense_uris,
            "raw": raw_block,
        })
    return header, blocks

# ========== FILTERING ==========

def check_entity(block: dict) -> tuple[bool, str]:
    """Returns (keep, reason). If keep=False, reason explains why."""
    uri = block["uri"]
    gloss = block["gloss"]
    label = block["label"]

    # No label at all
    if not label:
        return False, f"NO_LABEL  {uri}"

    # Gloss-based rejection
    if gloss:
        for pattern, category in _GLOSS_REJECT_COMPILED:
            if pattern.search(gloss):
                return False, f"{category}  {uri}  gloss={gloss!r}"

    # No gloss — could be anything. Keep only if label looks product-like.
    # Entities with no gloss and a very generic/short label are suspect.
    # But removing all no-gloss entities is too aggressive — many are real.
    # We'll keep them unless they match label-based heuristics.

    return True, ""

# ========== MAIN ==========

def main():
    if not os.path.exists(ENTITY_FILE):
        sys.exit(f"❌ Entity file not found: {ENTITY_FILE}")
    if not os.path.exists(ENTRY_FILE):
        sys.exit(f"❌ Entry file not found: {ENTRY_FILE}")

    print(f"📂 Entity file: {ENTITY_FILE}")
    print(f"📂 Entry file:  {ENTRY_FILE}")
    print()

    # --- Parse ---
    print("Parsing entity file…")
    entity_header, entity_blocks = parse_entity_blocks(ENTITY_FILE)
    print(f"   {len(entity_blocks):,} entities parsed")

    print("Parsing entry file…")
    entry_header, entry_blocks = parse_entry_blocks(ENTRY_FILE)
    print(f"   {len(entry_blocks):,} entries parsed")
    print()

    # --- Filter entities ---
    print("Filtering entities…")
    kept_entities: list[dict] = []
    removed_uris: set[str] = set()
    removal_log: list[str] = []
    category_counts: dict[str, int] = {}

    for block in entity_blocks:
        keep, reason = check_entity(block)
        if keep:
            kept_entities.append(block)
        else:
            removed_uris.add(block["uri"])
            removal_log.append(reason)
            cat = reason.split()[0] if reason else "UNKNOWN"
            category_counts[cat] = category_counts.get(cat, 0) + 1

    removed_count = len(entity_blocks) - len(kept_entities)
    print(f"   ✅ Kept:    {len(kept_entities):,}")
    print(f"   🚫 Removed: {removed_count:,}")
    print()

    print("   Removal breakdown:")
    for cat, cnt in sorted(category_counts.items(), key=lambda x: -x[1]):
        print(f"      {cnt:>6,}  {cat}")
    print()

    # --- Cascade into entries ---
    print("Cascading into entries…")
    kept_entries: list[dict] = []
    entries_dropped = 0
    entries_trimmed = 0

    for block in entry_blocks:
        original_senses = block["sense_uris"]
        surviving_senses = [s for s in original_senses if s not in removed_uris]

        if not surviving_senses:
            entries_dropped += 1
            continue

        if len(surviving_senses) < len(original_senses):
            # Rewrite the sense line in the raw block
            old_sense_str = ", ".join(original_senses)
            new_sense_str = ", ".join(surviving_senses)
            block["raw"] = block["raw"].replace(old_sense_str, new_sense_str)
            block["sense_uris"] = surviving_senses
            entries_trimmed += 1

        kept_entries.append(block)

    print(f"   ✅ Kept:    {len(kept_entries):,}")
    print(f"   🗑️  Dropped (no senses left): {entries_dropped:,}")
    print(f"   ✂️  Trimmed (dead senses removed): {entries_trimmed:,}")
    print()

    # --- Write outputs ---
    # Backup originals
    for path in [ENTITY_FILE, ENTRY_FILE]:
        bak = path + ".bak"
        if not os.path.exists(bak):
            os.rename(path, bak)
            print(f"   Backed up → {bak}")
        else:
            print(f"   Backup already exists: {bak}")

    # Write cleaned entity file
    with open(ENTITY_FILE, "w", encoding="utf-8") as f:
        f.write(entity_header)
        for block in kept_entities:
            f.write("\n" + block["raw"] + "\n")
    print(f"✅ Wrote {ENTITY_FILE}  ({len(kept_entities):,} entities)")

    # Write cleaned entry file
    with open(ENTRY_FILE, "w", encoding="utf-8") as f:
        f.write(entry_header)
        for block in kept_entries:
            f.write("\n" + block["raw"] + "\n")
    print(f"✅ Wrote {ENTRY_FILE}  ({len(kept_entries):,} entries)")

    # Write removal log
    with open(REMOVAL_LOG, "w", encoding="utf-8") as f:
        f.write(f"# Product cleaner removal log — {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"# Entities removed: {removed_count}\n")
        f.write(f"# Entries dropped:  {entries_dropped}\n")
        f.write(f"# Entries trimmed:  {entries_trimmed}\n\n")
        for line in removal_log:
            f.write(line + "\n")
    print(f"📝 Removal log → {REMOVAL_LOG}")

if __name__ == "__main__":
    main()