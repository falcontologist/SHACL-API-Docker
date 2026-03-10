#!/usr/bin/env python3
"""
Parse entity + entry TTL files directly and produce TSV dumps for the FST builder.
Bypasses Virtuoso SPARQL entirely — no timeouts, no pagination, no ORDER BY issues.

Usage:
    python3 dump_ttl_to_tsv.py <category> <entry.ttl> <entity.ttl> <output.tsv>

Example:
    python3 dump_ttl_to_tsv.py Creative_Work creative_work_entry.ttl creative_work_entity.ttl creative_work_dump.tsv
"""

import sys
import re
import time

ONT = "https://falcontologist.github.io/shacl-demo/ontology/"

def parse_entities(filepath):
    """Parse entity TTL file into dict: entity_iri -> {label, gloss, identifier}"""
    entities = {}
    current_iri = None
    current = {}

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('@prefix'):
                continue

            # New subject
            m = re.match(r'^:(\S+)\s+a\s+:', line)
            if m:
                if current_iri and current:
                    entities[current_iri] = current
                current_iri = ONT + m.group(1)
                current = {}
                continue

            # rdfs:label
            m = re.match(r'rdfs:label\s+"(.+?)"', line)
            if m:
                current['label'] = m.group(1)
                continue

            # :gloss
            m = re.match(r':gloss\s+"(.+?)"', line)
            if m:
                current['gloss'] = m.group(1)
                continue

            # :identifier
            m = re.match(r':identifier\s+(?:wiki:|<http\S+?>)', line)
            if m:
                # Extract full IRI
                m2 = re.match(r':identifier\s+wiki:(\S+)', line)
                if m2:
                    current['identifier'] = "http://www.wikidata.org/entity/" + m2.group(1).rstrip(' ;.')
                else:
                    m2 = re.match(r':identifier\s+<(\S+?)>', line)
                    if m2:
                        current['identifier'] = m2.group(1)
                continue

        # Don't forget the last entity
        if current_iri and current:
            entities[current_iri] = current

    return entities


def parse_entries_and_write(entry_filepath, entities, output_filepath):
    """Parse entry TTL file and write TSV joining with entity data."""
    count = 0
    missing = 0

    with open(entry_filepath, 'r', encoding='utf-8') as f_in, \
         open(output_filepath, 'w', encoding='utf-8') as f_out:

        f_out.write("entry\tlabel\tentityIRI\tentityLabel\tgloss\tidentifier\n")

        current_iri = None
        current_label = None
        current_sense = None

        for line in f_in:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('@prefix'):
                continue

            # New subject
            m = re.match(r'^:(\S+)\s+a\s+:', line)
            if m:
                # Write previous entry
                if current_iri and current_label and current_sense:
                    entity = entities.get(current_sense, {})
                    entity_label = entity.get('label', '')
                    gloss = entity.get('gloss', '')
                    identifier = entity.get('identifier', '')
                    f_out.write(f"{current_iri}\t{current_label}\t{current_sense}\t{entity_label}\t{gloss}\t{identifier}\n")
                    count += 1
                    if not entity:
                        missing += 1
                    if count % 500000 == 0:
                        print(f"  {count:,} entries written...")

                current_iri = ONT + m.group(1)
                current_label = None
                current_sense = None
                continue

            # rdfs:label
            m = re.match(r'rdfs:label\s+"(.+?)"', line)
            if m:
                current_label = m.group(1)
                continue

            # :sense
            m = re.match(r':sense\s+:(\S+)', line)
            if m:
                current_sense = ONT + m.group(1).rstrip(' ;.')
                continue

        # Last entry
        if current_iri and current_label and current_sense:
            entity = entities.get(current_sense, {})
            entity_label = entity.get('label', '')
            gloss = entity.get('gloss', '')
            identifier = entity.get('identifier', '')
            f_out.write(f"{current_iri}\t{current_label}\t{current_sense}\t{entity_label}\t{gloss}\t{identifier}\n")
            count += 1

    return count, missing


def main():
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} <category> <entry.ttl> <entity.ttl> <output.tsv>")
        sys.exit(1)

    category = sys.argv[1]
    entry_file = sys.argv[2]
    entity_file = sys.argv[3]
    output_file = sys.argv[4]

    print(f"=== TTL to TSV: {category} ===")

    print(f"Parsing entities from {entity_file}...")
    t0 = time.time()
    entities = parse_entities(entity_file)
    t1 = time.time()
    print(f"  {len(entities):,} entities parsed in {t1-t0:.1f}s")

    print(f"Parsing entries from {entry_file} and writing {output_file}...")
    count, missing = parse_entries_and_write(entry_file, entities, output_file)
    t2 = time.time()
    print(f"  {count:,} entries written in {t2-t1:.1f}s ({missing} with missing entity data)")
    print(f"  Total time: {t2-t0:.1f}s")


if __name__ == "__main__":
    main()
