from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import rdflib
from rdflib import Namespace
from rdflib.namespace import RDF, RDFS

app = FastAPI()

# Enable CORS for CodePen
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURATION ---
# Points to your clean ontology as the source of truth
ONTOLOGY_FILE = "roles_ontology_clean.ttl"
SHACL_FILE = "roles_shacl.ttl"

# Load the Graphs
print("Loading Ontology...")
ONT_GRAPH = rdflib.Graph()
ONT_GRAPH.parse(ONTOLOGY_FILE, format="turtle")

print("Loading SHACL Shapes...")
SHACL_GRAPH = rdflib.Graph()
SHACL_GRAPH.parse(SHACL_FILE, format="turtle")

# Define Namespaces
SH = Namespace("http://www.w3.org/ns/shacl#")
# We use the empty prefix from your clean ontology
ONT = Namespace("http://example.org/ontology/")

# --- DATA MODELS ---
class ValidateRequest(BaseModel):
    turtle_data: str

# --- ENDPOINTS ---

@app.get("/api/forms")
def get_forms():
    """
    Returns form definitions based on SHACL shapes.
    """
    forms = {}
    
    # Find all NodeShapes
    for shape in SHACL_GRAPH.subjects(RDF.type, SH.NodeShape):
        # Get the target class (e.g., :Possession)
        target_class = SHACL_GRAPH.value(shape, SH.targetClass)
        if not target_class:
            continue
            
        shape_name = str(target_class).split("/")[-1]
        fields = []
        
        # Find all property constraints
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
    Searches the ontology for a verb and returns its Situation 
    and Semantic Domain fallback.
    """
    verb_uri = ONT[verb.lower().replace(" ", "_")]
    
    # Query: Find what this verb evokes and its domain
    # We look for: :verb :evokes ?sit . :verb :semantic_domain ?domain
    results = []
    
    # 1. Find evoked situations
    situations = list(ONT_GRAPH.objects(verb_uri, ONT.evokes))
    
    if not situations:
        return {"found": False, "message": "Verb not found in ontology."}

    for sit in situations:
        sit_name = str(sit).split("/")[-1]
        
        # 2. Find the Semantic Domain for this verb (The Fallback)
        # Note: In your ontology, the domain is attached to the VERB, not always the situation subclass
        domain = ONT_GRAPH.value(verb_uri, ONT.semantic_domain)
        
        domain_name = None
        if domain:
            domain_name = str(domain).split("/")[-1]
            
        results.append({
            "situation": sit_name,      # The specific event (e.g. Dynamic_Possession)
            "fallback_domain": domain_name, # The Shape to use (e.g. Possession)
            "vn_class": "Unknown"       # Placeholder if needed later
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

    # Run Validation
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
