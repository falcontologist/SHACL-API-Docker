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
        app.get("/api/forms", App::getForms);
        app.post("/api/infer", App::inferGraph);
        app.post("/api/validate", App::validateGraph);
    }

    // --- 1. RESTORED LOADING LOGIC (From your working commit) ---
    private static Model loadShapesGraph() {
        try {
            Model m = JenaUtil.createMemoryModel();
            // This was the specific line from your working version
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
        long roles = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.PropertyShape).toList().size();
        
        long lemmas = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, 
                SHAPES_GRAPH.createResource(ONT_NS + "Lemma")).toList().size();
        long synsets = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, 
                SHAPES_GRAPH.createResource(ONT_NS + "Synset")).toList().size();

        ctx.json(Map.of(
                "shapes", shapes,
                "roles", roles,
                "lemmas", lemmas,
                "senses", synsets,
                "rules", 0 // Simplification as per original
        ));
    }

    private static void lookupVerb(Context ctx) {
        String verb = ctx.queryParam("verb");
        if (verb == null) {
            ctx.status(400).result("Missing verb parameter");
            return;
        }

        ResIterator lemmaIter = SHAPES_GRAPH.listSubjectsWithProperty(
                SHAPES_GRAPH.createProperty(ONT_NS + "lemma"), verb);
        
        if (!lemmaIter.hasNext()) {
            ctx.json(Map.of("found", false));
            return;
        }

        Resource lemmaRes = lemmaIter.next();
        List<Map<String, Object>> senses = new ArrayList<>();

        ResIterator senseIter = SHAPES_GRAPH.listSubjectsWithProperty(
                SHAPES_GRAPH.createProperty(ONT_NS + "word"), lemmaRes);

        while (senseIter.hasNext()) {
            Resource senseRes = senseIter.next();
            String gloss = "";
            if (senseRes.hasProperty(SHAPES_GRAPH.createProperty(ONT_NS + "gloss"))) {
                gloss = senseRes.getProperty(SHAPES_GRAPH.createProperty(ONT_NS + "gloss")).getString();
            }

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

    private static void getForms(Context ctx) {
        Map<String, Object> forms = new HashMap<>();

        ResIterator shapes = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape);
        while (shapes.hasNext()) {
            Resource shape = shapes.next();
            String shapeName = shape.getLocalName();
            if (shapeName == null) continue;

            // Added back the targetClass logic because the FRONTEND relies on it now
            // to fix the "Shape vs Class" mismatch.
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
            
            if (!fields.isEmpty()) {
                forms.put(shapeName, Map.of(
                    "targetClass", targetClass,
                    "fields", fields
                ));
            }
        }

        ctx.json(Map.of("forms", forms));
    }

    // --- 2. THE CRITICAL FIX FOR "DISAPPEARING GRAPH" ---
    private static void inferGraph(Context ctx) {
        String ttlInput = ctx.body();
        Model dataModel = JenaUtil.createMemoryModel();

        try {
            dataModel.read(new ByteArrayInputStream(ttlInput.getBytes(StandardCharsets.UTF_8)), null, "TTL");
            long originalSize = dataModel.size();
            System.out.println("[infer] Loaded input: " + originalSize + " triples");

            // Dictionary Injection (Required for rules to fire)
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
                    StmtIterator props = lemmaNode.listProperties();
                    while (props.hasNext()) {
                        dataModel.add(props.next());
                    }
                    lemmaNodesAdded++;
                }
            }

            System.out.println("[infer] Executing SHACL rules...");
            Model inferredModel = RuleUtil.executeRules(dataModel, SHAPES_GRAPH, null, null);
            long newInferredTriples = inferredModel.size(); 
            
            // MERGE: This keeps your graph from disappearing
            dataModel.add(inferredModel);

            System.out.println("[infer] Generated " + newInferredTriples + " new triples");

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

    private static void validateGraph(Context ctx) {
        String ttlInput = ctx.body();
        Model dataModel = JenaUtil.createMemoryModel();

        try {
            dataModel.read(new ByteArrayInputStream(ttlInput.getBytes(StandardCharsets.UTF_8)), null, "TTL");
            
            Resource report = ValidationUtil.validateModel(dataModel, SHAPES_GRAPH, false);
            
            StringWriter reportWriter = new StringWriter();
            RDFDataMgr.write(reportWriter, report.getModel(), RDFFormat.TURTLE);

            boolean conforms = report.getProperty(SH.conforms).getBoolean();

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
