from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import rdflib
from rdflib import Namespace
from rdflib.namespace import RDF, RDFS

app = FastAPI()

# Enable CORS for CodePen access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURATION ---
# Fetch the ontology from your data repo
ONTOLOGY_URL = "https://raw.githubusercontent.com/falcontologist/Verbs_to_Situations/main/roles_ontology_clean.ttl"
# SHACL shapes remain in the local API repo
SHACL_FILE = "roles_shacl.ttl"

# Load the Graphs
print("Loading Ontology from GitHub...")
ONT_GRAPH = rdflib.Graph()
try:
    ONT_GRAPH.parse(location=ONTOLOGY_URL, format="turtle")
    print("Ontology loaded successfully.")
except Exception as e:
    print(f"Error loading ontology: {e}")

print("Loading SHACL Shapes...")
SHACL_GRAPH = rdflib.Graph()
try:
    SHACL_GRAPH.parse(SHACL_FILE, format="turtle")
    print("SHACL shapes loaded successfully.")
except Exception as e:
    print(f"Error loading SHACL file: {e}")

# Define Namespaces
SH = Namespace("http://www.w3.org/ns/shacl#")
ONT = Namespace("http://example.org/ontology/")

# --- DATA MODELS ---
class ValidateRequest(BaseModel):
    turtle_data: str

# --- ENDPOINTS ---

@app.get("/api/forms")
def get_forms():
    """
    Returns form definitions based on SHACL shapes found in roles_shacl.ttl
    """
    forms = {}
    for shape in SHACL_GRAPH.subjects(RDF.type, SH.NodeShape):
        target_class = SHACL_GRAPH.value(shape, SH.targetClass)
        if not target_class:
            continue
            
        shape_name = str(target_class).split("/")[-1]
        fields = []
        
        for prop in SHACL_GRAPH.objects(shape, SH.property):
            path = SHACL_GRAPH.value(prop, SH.path)
            name = SHACL_GRAPH.value(prop, SH.name)
            min_count = SHACL_GRAPH.value(prop, SH.minCount)
            
            if path and name:
                fields.append({
                    "path": str(path),
                    "name": str(name),
                    "required": (min_count is not None and int(min_count) > 0)
                })
        forms[shape_name] = fields
    return {"forms": forms}

@app.get("/api/lookup")
def lookup_verb(verb: str):
    """
    Queries the remote ontology for a verb's Situation and Semantic Domain.
    """
    verb_clean = verb.lower().strip().replace(" ", "_")
    verb_uri = ONT[verb_clean]
    
    results = []
    
    # Find all situations the verb evokes
    situations = list(ONT_GRAPH.objects(verb_uri, ONT.evokes))
    
    if not situations:
        # Fallback check: maybe the verb exists but doesn't have an evokes triple yet
        if (verb_uri, RDF.type, ONT.Verb) in ONT_GRAPH:
             return {"found": True, "verb": verb, "mappings": [], "message": "Verb exists but no situations mapped."}
        return {"found": False, "message": f"Verb '{verb}' not found in ontology."}

    for sit in situations:
        sit_name = str(sit).split("/")[-1]
        
        # Find the Semantic Domain (used for SHACL form fallback)
        domain = ONT_GRAPH.value(verb_uri, ONT.semantic_domain)
        domain_name = str(domain).split("/")[-1] if domain else None
        
        # Get VN Class if available
        vn = ONT_GRAPH.value(verb_uri, ONT.vn_class)
        vn_name = str(vn).split("/")[-1] if vn else "Unknown"

        results.append({
            "situation": sit_name,
            "fallback_domain": domain_name,
            "vn_class": vn_name
        })

    return {
        "found": True, 
        "verb": verb, 
        "mappings": results
    }

@app.post("/api/validate")
def validate_graph(request: ValidateRequest):
    from pyshacl import validate
    data_graph = rdflib.Graph()
    try:
        data_graph.parse(data=request.turtle_data, format="turtle")
    except Exception as e:
        return {"conforms": False, "detail": str(e)}

    conforms, report_graph, report_text = validate(
        data_graph,
        shacl_graph=SHACL_GRAPH,
        ont_graph=ONT_GRAPH,
        inference='rdfs',
        abort_on_first=False,
        meta_shacl=False,
        debug=False
    )
    
    return {
        "conforms": conforms,
        "report_text": report_text
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
