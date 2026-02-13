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
# Single Source of Truth: This file now contains BOTH Ontology and SHACL Shapes
DATA_FILE = "roles_shacl.ttl" 

# Load the Unified Graph
print(f"Loading unified data from {DATA_FILE}...")
UNIFIED_GRAPH = rdflib.Graph()
try:
    UNIFIED_GRAPH.parse(DATA_FILE, format="turtle")
    print("Graph loaded successfully.")
except Exception as e:
    print(f"Error loading graph: {e}")

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
    Returns form definitions based on SHACL NodeShapes in the unified graph.
    """
    forms = {}
    for shape in UNIFIED_GRAPH.subjects(RDF.type, SH.NodeShape):
        target_class = UNIFIED_GRAPH.value(shape, SH.targetClass)
        if not target_class:
            continue
            
        shape_name = str(target_class).split("/")[-1]
        fields = []
        
        for prop in UNIFIED_GRAPH.objects(shape, SH.property):
            path = UNIFIED_GRAPH.value(prop, SH.path)
            name = UNIFIED_GRAPH.value(prop, SH.name)
            min_count = UNIFIED_GRAPH.value(prop, SH.minCount)
            
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
    Queries the unified graph for a verb's Situation and Semantic Domain.
    """
    verb_clean = verb.lower().strip().replace(" ", "_")
    verb_uri = ONT[verb_clean]
    
    results = []
    
    # 1. Find all situations the verb evokes
    situations = list(UNIFIED_GRAPH.objects(verb_uri, ONT.evokes))
    
    if not situations:
        # Fallback: Check if verb exists at all
        if (verb_uri, RDF.type, ONT.Verb) in UNIFIED_GRAPH:
             return {"found": True, "verb": verb, "mappings": [], "message": "Verb exists but no situations mapped."}
        return {"found": False, "message": f"Verb '{verb}' not found in ontology."}

    for sit in situations:
        sit_name = str(sit).split("/")[-1]
        
        # 2. Find the Semantic Domain (Fallback for SHACL)
        domain = UNIFIED_GRAPH.value(verb_uri, ONT.semantic_domain)
        domain_name = str(domain).split("/")[-1] if domain else None
        
        # 3. Get VN Class
        vn = UNIFIED_GRAPH.value(verb_uri, ONT.vn_class)
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

    # Validate using the Unified Graph as both the SHACL file and the Ontology file
    conforms, report_graph, report_text = validate(
        data_graph,
        shacl_graph=UNIFIED_GRAPH,
        ont_graph=UNIFIED_GRAPH,
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
