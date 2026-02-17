package com.example;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.topbraid.jenax.util.JenaUtil;
import org.topbraid.shacl.rules.RuleUtil;
import org.topbraid.shacl.validation.ValidationUtil;
import org.topbraid.shacl.vocabulary.SH;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class App {

    private static final String ONT_NS = "http://example.org/ontology/";
    private static final Model SHAPES_GRAPH = loadShapesGraph();

    public static void main(String[] args) {
        Javalin app = Javalin.create(config -> {
            config.bundledPlugins.enableCors(cors -> {
                cors.addRule(it -> it.anyHost());
            });
        }).start(8080);

        // API Endpoints
        app.get("/api/stats", App::getStats);
        app.get("/api/lookup", App::lookupVerb);
        app.get("/api/forms", App::getForms);       // UPDATED: Returns targetClass
        app.post("/api/infer", App::inferGraph);    // UPDATED: Merges inference
        app.post("/api/validate", App::validateGraph);
    }

    private static Model loadShapesGraph() {
        try {
            Model m = JenaUtil.createMemoryModel();
            // Load the main ontology file
            InputStream is = App.class.getResourceAsStream("/roles_shacl.ttl");
            if (is == null) throw new RuntimeException("roles_shacl.ttl not found in resources");
            m.read(is, null, "TTL");
            System.out.println("Loaded SHACL Shapes Graph: " + m.size() + " triples");
            return m;
        } catch (Exception e) {
            e.printStackTrace();
            return JenaUtil.createMemoryModel();
        }
    }

    private static void getStats(Context ctx) {
        long shapes = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).toList().size();
        // Count roles (PropertyShapes)
        long roles = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.PropertyShape).toList().size();
        
        // Custom counts based on your ontology structure
        long lemmas = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, 
                SHAPES_GRAPH.createResource(ONT_NS + "Lemma")).toList().size();
        long synsets = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, 
                SHAPES_GRAPH.createResource(ONT_NS + "Synset")).toList().size();

        ctx.json(Map.of(
                "shapes", shapes,
                "roles", roles,
                "lemmas", lemmas,
                "senses", synsets,
                "rules", 0 // Placeholder or calculate if needed
        ));
    }

    /**
     * LOOKUP VERB - Finds Lemma and Senses
     */
    private static void lookupVerb(Context ctx) {
        String verb = ctx.queryParam("verb");
        if (verb == null) {
            ctx.status(400).result("Missing verb parameter");
            return;
        }

        // 1. Find the Lemma
        ResIterator lemmaIter = SHAPES_GRAPH.listSubjectsWithProperty(
                SHAPES_GRAPH.createProperty(ONT_NS + "lemma"), verb);
        
        if (!lemmaIter.hasNext()) {
            ctx.json(Map.of("found", false));
            return;
        }

        Resource lemmaRes = lemmaIter.next();
        List<Map<String, Object>> senses = new ArrayList<>();

        // 2. Find Senses linked to this Lemma
        ResIterator senseIter = SHAPES_GRAPH.listSubjectsWithProperty(
                SHAPES_GRAPH.createProperty(ONT_NS + "word"), lemmaRes);

        while (senseIter.hasNext()) {
            Resource senseRes = senseIter.next();
            String gloss = "";
            if (senseRes.hasProperty(SHAPES_GRAPH.createProperty(ONT_NS + "gloss"))) {
                gloss = senseRes.getProperty(SHAPES_GRAPH.createProperty(ONT_NS + "gloss")).getString();
            }

            // Find valid situations (Shapes) for this sense
            List<String> validSituations = new ArrayList<>();
            StmtIterator sitIter = senseRes.listProperties(SHAPES_GRAPH.createProperty(ONT_NS + "valid_situation"));
            while (sitIter.hasNext()) {
                validSituations.add(sitIter.next().getResource().getLocalName());
            }

            senses.add(Map.of(
                    "id", senseRes.getLocalName(),
                    "gloss", gloss,
                    "situations", validSituations
            ));
        }

        ctx.json(Map.of(
                "found", true,
                "lemma", lemmaRes.getLocalName(),
                "senses", senses
        ));
    }

    /**
     * GET FORMS - Generates UI config from SHACL
     * FIX: Now extracts sh:targetClass to send to frontend
     */
    private static void getForms(Context ctx) {
        Map<String, Object> forms = new HashMap<>();

        ResIterator shapes = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape);
        while (shapes.hasNext()) {
            Resource shape = shapes.next();
            String shapeName = shape.getLocalName();
            if (shapeName == null) continue;

            // 1. Extract the Target Class (The SHACL Way)
            // If the shape defines sh:targetClass, use it. 
            // Otherwise default to the shapeName (fallback).
            String targetClass = shapeName;
            if (shape.hasProperty(SH.targetClass)) {
                targetClass = shape.getPropertyResourceValue(SH.targetClass).getLocalName();
            }

            List<Map<String, Object>> fields = new ArrayList<>();
            StmtIterator props = shape.listProperties(SH.property);
            
            while (props.hasNext()) {
                Resource propertyShape = props.next().getResource();

                String path = propertyShape.hasProperty(SH.path)
                        ? propertyShape.getPropertyResourceValue(SH.path).getLocalName()
                        : "unknown";

                String name = propertyShape.hasProperty(SH.name)
                        ? propertyShape.getProperty(SH.name).getString()
                        : path;

                boolean required = propertyShape.hasProperty(SH.minCount)
                        && propertyShape.getProperty(SH.minCount).getInt() > 0;

                fields.add(Map.of(
                        "label", name,
                        "path", path,
                        "required", required
                ));
            }
            
            // Only include shapes that have fields defined
            if (!fields.isEmpty()) {
                forms.put(shapeName, Map.of(
                    "targetClass", targetClass,
                    "fields", fields
                ));
            }
        }

        ctx.json(Map.of("forms", forms));
    }

    /**
     * INFER GRAPH - Runs SHACL SPARQL rules
     * FIX: Merges inference results back into the original data model so the graph persists.
     */
    private static void inferGraph(Context ctx) {
        String ttlInput = ctx.body();
        Model dataModel = JenaUtil.createMemoryModel();

        try {
            // 1. Load user input
            dataModel.read(new ByteArrayInputStream(ttlInput.getBytes(StandardCharsets.UTF_8)), null, "TTL");
            long originalSize = dataModel.size();
            System.out.println("[infer] Loaded input: " + originalSize + " triples");

            // 2. Dictionary Logic: Find Lemma nodes and inject them
            // The frontend sends :lemma "verb". We need to find the Ontology Lemma node 
            // and add its properties (like :present3sg) to the data model so rules can read them.
            Set<String> lemmasNeeded = new HashSet<>();
            Property lemmaProp = SHAPES_GRAPH.createProperty(ONT_NS + "lemma");
            
            StmtIterator lemmaStmts = dataModel.listStatements(null, lemmaProp, (RDFNode) null);
            while (lemmaStmts.hasNext()) {
                Statement stmt = lemmaStmts.next();
                if (stmt.getObject().isLiteral()) {
                    lemmasNeeded.add(stmt.getObject().asLiteral().getString());
                }
            }
            
            int lemmaNodesAdded = 0;
            for (String lemmaStr : lemmasNeeded) {
                ResIterator lemmaNodes = SHAPES_GRAPH.listSubjectsWithProperty(lemmaProp, lemmaStr);
                while (lemmaNodes.hasNext()) {
                    Resource lemmaNode = lemmaNodes.next();
                    // Copy all properties of the Lemma node (e.g., :present3sg) into dataModel
                    StmtIterator props = lemmaNode.listProperties();
                    while (props.hasNext()) {
                        dataModel.add(props.next());
                    }
                    lemmaNodesAdded++;
                }
            }

            // 3. Run SHACL inference
            // executeRules returns ONLY the new inferred triples (Deductions Model)
            System.out.println("[infer] Executing SHACL rules...");
            Model inferredModel = RuleUtil.executeRules(dataModel, SHAPES_GRAPH, null, null);
            long newInferredTriples = inferredModel.size(); 
            
            // 4. CRITICAL FIX: Merge inferences back into the main data model
            dataModel.add(inferredModel);

            System.out.println("[infer] Generated " + newInferredTriples + " new triples");
            System.out.println("[infer] Complete. Total: " + dataModel.size() + " triples");

            // 5. Return the merged graph
            StringWriter dataWriter = new StringWriter();
            RDFDataMgr.write(dataWriter, dataModel, RDFFormat.TURTLE);

            ctx.json(Map.of(
                    "success", true,
                    "inferred_data", dataWriter.toString(),
                    "stats", Map.of(
                            "input_triples", originalSize,
                            "lemma_nodes_added", lemmaNodesAdded,
                            "inferred_triples", newInferredTriples,
                            "total_triples", dataModel.size()
                    )
            ));

        } catch (Exception e) {
            System.err.println("[infer] Error: " + e.getMessage());
            e.printStackTrace();
            ctx.status(400).result("Error during inference: " + e.getMessage());
        }
    }

    /**
     * VALIDATE GRAPH - Runs SHACL validation only (no inference)
     */
    private static void validateGraph(Context ctx) {
        String ttlInput = ctx.body();
        Model dataModel = JenaUtil.createMemoryModel();

        try {
            dataModel.read(new ByteArrayInputStream(ttlInput.getBytes(StandardCharsets.UTF_8)), null, "TTL");
            System.out.println("[validate] Loaded input: " + dataModel.size() + " triples");

            Resource report = ValidationUtil.validateModel(dataModel, SHAPES_GRAPH, false);
            
            StringWriter reportWriter = new StringWriter();
            RDFDataMgr.write(reportWriter, report.getModel(), RDFFormat.TURTLE);

            boolean conforms = report.getProperty(SH.conforms).getBoolean();
            System.out.println("[validate] Complete. Conforms: " + conforms);

            ctx.json(Map.of(
                    "conforms", conforms,
                    "report_text", reportWriter.toString()
            ));

        } catch (Exception e) {
            System.err.println("[validate] Error: " + e.getMessage());
            e.printStackTrace();
            ctx.status(400).result("Error during validation: " + e.getMessage());
        }
    }
}
