package com.example;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.topbraid.shacl.rules.RuleUtil;
import org.topbraid.shacl.validation.ValidationUtil;
import org.topbraid.shacl.vocabulary.SH;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class App {
    // Single Source of Truth: Loads Ontology + Shapes + Rules
    private static final Model UNIFIED_GRAPH = RDFDataMgr.loadModel("roles_shacl.ttl");
    private static final String ONT_NS = "http://example.org/ontology/";

    public static void main(String[] args) {
        Javalin app = Javalin.create(config -> {
            config.bundledPlugins.enableCors(cors -> cors.addRule(it -> it.anyHost()));
        }).start(8000);

        app.get("/", ctx -> ctx.result("TopBraid SHACL API Online"));
        app.get("/api/stats", App::getStats);
        app.get("/api/forms", App::getForms);
        app.get("/api/lookup", App::lookupVerb);
        app.post("/api/validate", App::validate);
    }

    private static void getForms(Context ctx) {
        Map<String, Object> response = new HashMap<>();
        Map<String, Map<String, Object>> forms = new HashMap<>();

        // Iterate over ALL NodeShapes in the graph
        UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).forEachRemaining(shape -> {
            if (!shape.hasProperty(SH.targetClass)) return;
            
            Resource targetClass = shape.getPropertyResourceValue(SH.targetClass);
            String name = targetClass.getLocalName();

            // 1. Initialize the Form Entry for this Target Class if missing
            forms.putIfAbsent(name, new HashMap<>(Map.of(
                "target_class", targetClass.getURI(),
                "fields", new ArrayList<Map<String, Object>>()
            )));

            Map<String, Object> formData = forms.get(name);
            List<Map<String, Object>> fields = (List<Map<String, Object>>) formData.get("fields");

            // 2. Track existing paths to prevent duplicates when merging multiple shapes
            Set<String> existingPaths = new HashSet<>();
            for(Map<String, Object> f : fields) existingPaths.add((String) f.get("path"));

            // 3. Extract properties from THIS shape and append to the list
            shape.listProperties(SH.property).forEachRemaining(p -> {
                Resource prop = p.getResource();
                if (!prop.hasProperty(SH.path)) return;
                
                String path = prop.getPropertyResourceValue(SH.path).getURI();
                if (existingPaths.contains(path)) return; // Skip dupes

                // Detect Field Type
                String type = "text";
                Resource datatype = prop.getPropertyResourceValue(SH.datatype);
                if (datatype != null && (datatype.equals(XSD.integer) || datatype.equals(XSD.xint) || datatype.equals(XSD.decimal))) {
                    type = "number";
                }
                
                // Get Label (fallback to local name)
                String label = prop.hasProperty(SH.name) ? prop.getProperty(SH.name).getString() : path.substring(path.lastIndexOf('/')+1);
                
                // Check Required
                boolean required = prop.hasProperty(SH.minCount) && prop.getProperty(SH.minCount).getInt() > 0;

                fields.add(Map.of(
                    "path", path, 
                    "label", label, 
                    "type", type, 
                    "required", required
                ));
            });
        });

        response.put("forms", forms);
        ctx.json(response);
    }

    private static void lookupVerb(Context ctx) {
        String query = ctx.queryParam("verb");
        if (query == null) { ctx.json(Map.of("found", false)); return; }
        String verb = query.trim().toLowerCase();

        List<Map<String, String>> mappings = new ArrayList<>();
        Property labelProp = RDFS.label;
        Property evokesProp = UNIFIED_GRAPH.createProperty(ONT_NS + "evokes");
        Property semanticDomainProp = UNIFIED_GRAPH.createProperty(ONT_NS + "semantic_domain");

        // Iterate all Subjects with an 'evokes' property (finding Verbs)
        ResIterator verbs = UNIFIED_GRAPH.listSubjectsWithProperty(evokesProp);
        while (verbs.hasNext()) {
            Resource v = verbs.next();
            // Match Verb Label
            if (v.hasProperty(labelProp) && v.getProperty(labelProp).getString().toLowerCase().equals(verb)) {
                
                // Get evoked Situations
                StmtIterator evokes = v.listProperties(evokesProp);
                while (evokes.hasNext()) {
                    Resource situation = evokes.next().getResource();
                    
                    // WORKFLOW LOGIC: Verb -> Situation -> Semantic Domain -> Shape
                    // If the Situation points to a Semantic Domain, use that as the form target.
                    // Otherwise, use the Situation itself.
                    Resource target = situation;
                    if (situation.hasProperty(semanticDomainProp)) {
                        target = situation.getPropertyResourceValue(semanticDomainProp);
                    }
                    
                    String name = target.getLocalName();
                    mappings.add(Map.of("situation", name, "fallback_domain", name));
                }
            }
        }
        ctx.json(Map.of("found", !mappings.isEmpty(), "mappings", mappings));
    }

    private static void validate(Context ctx) {
        try {
            // 1. Load User Data
            Model dataModel = ModelFactory.createDefaultModel();
            dataModel.read(new ByteArrayInputStream(ctx.bodyAsBytes()), null, "TURTLE");

            // 2. EXECUTE RULES (TopBraid Engine)
            // This runs SPARQL Rules found in UNIFIED_GRAPH against the user's data
            Model inferred = RuleUtil.executeRules(dataModel, UNIFIED_GRAPH, null, null);
            dataModel.add(inferred);

            // 3. VALIDATE
            Resource report = ValidationUtil.validateModel(dataModel, UNIFIED_GRAPH, true);
            
            // Serialize outputs
            StringWriter swReport = new StringWriter();
            RDFDataMgr.write(swReport, report.getModel(), RDFFormat.TURTLE);
            
            StringWriter swData = new StringWriter();
            RDFDataMgr.write(swData, dataModel, RDFFormat.TURTLE);

            ctx.json(Map.of(
                "conforms", report.getProperty(SH.conforms).getBoolean(),
                "report_text", swReport.toString(),
                "expanded_data", swData.toString()
            ));
        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).result(e.getMessage());
        }
    }

    private static void getStats(Context ctx) {
        long shapes = UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).toList().size();
        
        Set<String> roles = new HashSet<>();
        UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).forEachRemaining(s -> 
            s.listProperties(SH.property).forEachRemaining(p -> {
                if(p.getResource().hasProperty(SH.path)) 
                    roles.add(p.getResource().getPropertyResourceValue(SH.path).getURI());
            })
        );
        
        long lemmas = UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, UNIFIED_GRAPH.createResource(ONT_NS + "Verb")).toList().size();
        long senses = UNIFIED_GRAPH.listStatements(null, UNIFIED_GRAPH.createProperty(ONT_NS + "evokes"), (RDFNode) null).toList().size();

        ctx.json(Map.of("shapes", shapes, "roles", roles.size(), "lemmas", lemmas, "senses", senses));
    }
}
