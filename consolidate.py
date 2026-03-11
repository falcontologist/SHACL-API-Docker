import re
from collections import defaultdict

def consolidate_gpe_entries(input_file, output_file):
    # Regex to capture blocks of data
    # It looks for: :[base_name].entry.[numbers] followed by its properties until a period.
    pattern = re.compile(
        r'^:([^.]+)\.entry\.\d+\s+a\s+:Geopolitical_Entry\s*;\s*(.*?)\.\s*$',
        re.MULTILINE | re.DOTALL
    )

    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Find where the entries start so we can preserve your ontology headers/prefixes
    first_match = pattern.search(content)
    if not first_match:
        print("No matching entries found. Please check your file format.")
        return

    header = content[:first_match.start()]
    
    # Dictionary to hold the merged properties. 
    # Structure: consolidated[base_name][property_name] = set_of_unique_values
    consolidated = defaultdict(lambda: defaultdict(set))

    for match in pattern.finditer(content):
        gpe_base = match.group(1) # e.g., 'Kingdom_of_Hawaii'
        body = match.group(2)     # The block of properties
        
        # Split by ';' to get individual property declarations
        properties = body.split(';')
        for prop in properties:
            prop = prop.strip()
            if not prop:
                continue
            
            # Split into the predicate (e.g., 'rdfs:label') and the object(s)
            parts = prop.split(None, 1)
            if len(parts) == 2:
                predicate, objs = parts
                
                # We handle rdfs:label carefully to ensure commas inside literal strings 
                # (like "Bahamas, The") don't accidentally get split.
                if predicate == 'rdfs:label':
                    # Extract quoted strings with optional language tags like @en
                    labels = re.findall(r'"[^"]+"(?:@[a-zA-Z\-]+)?', objs)
                    consolidated[gpe_base][predicate].update(labels)
                else:
                    # For URIs (:sense, :pos, :source), we can safely split by comma
                    obj_list = [o.strip() for o in objs.split(',')]
                    consolidated[gpe_base][predicate].update(obj_list)

    # Write the consolidated data out to the new file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(header)
        
        # Sort alphabetically by the base name to keep the file deterministic and clean
        for gpe_base, properties in sorted(consolidated.items()):
            f.write(f":{gpe_base}.gpe_entry a :Geopolitical_Entry ;\n")
            
            prop_lines = []
            
            # Keep a neat, logical order for the properties
            order = ['rdfs:label', ':sense', ':pos', ':source']
            all_preds = set(properties.keys())
            ordered_preds = [p for p in order if p in all_preds] + [p for p in all_preds if p not in order]
            
            for pred in ordered_preds:
                # Sort values for deterministic output and join with commas
                vals = sorted(list(properties[pred]))
                prop_lines.append(f"    {pred} {', '.join(vals)}")
                
            f.write(" ;\n".join(prop_lines))
            f.write(" .\n\n")
            
    print(f"Successfully consolidated entries. Saved to {output_file}")

if __name__ == "__main__":
    # Point these to your desired file paths
    consolidate_gpe_entries('gpe_entry.ttl', 'gpe_entry_consolidated.ttl')