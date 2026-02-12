from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import rdflib
from pyshacl import validate

app = FastAPI()

# Allow your CodePen to talk to this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to codepen.io
    allow_methods=["*"],
    allow_headers=["*"],
)

SHACL_FILE = "roles_shacl.ttl" # Path to your source of truth
SHACL_GRAPH = rdflib.Graph().parse(SHACL_FILE, format="turtle")
SH = rdflib.Namespace("http://www.w3.org/ns/shacl#")

class ValidationRequest(BaseModel):
    turtle_data: str

@app.get("/api/forms")
def get_form_schema():
    """
    Parses the SHACL graph securely using RDFLib to generate a clean JSON 
    schema for the CodePen frontend to render dynamically.
    """
    schema = {}
    for shape in SHACL_GRAPH.subjects(rdflib.RDF.type, SH.NodeShape):
        target_class = SHACL_GRAPH.value(shape, SH.targetClass)
        if not target_class: continue
        
        domain_name = str(target_class).split("/")[-1]
        schema[domain_name] = []
        
        for prop in SHACL_GRAPH.objects(shape, SH.property):
            name = str(SHACL_GRAPH.value(prop, SH.name))
            min_count = SHACL_GRAPH.value(prop, SH.minCount)
            
            schema[domain_name].append({
                "name": name,
                "required": True if min_count and int(min_count) > 0 else False
            })
            
    return {"forms": schema}

@app.post("/api/validate")
def validate_graph(req: ValidationRequest):
    """
    Accepts Turtle data from CodePen and runs it through PySHACL.
    """
    try:
        data_graph = rdflib.Graph().parse(data=req.turtle_data, format="turtle")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid Turtle syntax: {str(e)}")

    # Run PySHACL standard validation
    conforms, report_graph, report_text = validate(
        data_graph,
        shacl_graph=SHACL_GRAPH,
        ont_graph=None,
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