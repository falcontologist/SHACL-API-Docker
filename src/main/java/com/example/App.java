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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
        app.post("/api/infer", App::inferGraph);        // NEW: Inference endpoint
        app.post("/api/validate", App::validateGraph);  // Validation only

        System.out.println("Server running on port 8080");
    }

    private static final String MANIFEST_URL =
        "https://raw.githubusercontent.com/falcontologist/SHACL-API-Docker/main/manifest.ttl";

    private static final String MANIFEST_NS = "http://example.org/ontology/";

    private static Model loadShapesGraph() {
        Model merged = JenaUtil.createMemoryModel();
        try {
            System.out.println("[startup] Loading manifest from: " + MANIFEST_URL);
            Model manifest = fetchTTL(MANIFEST_URL);

            Property loadOrderProp  = manifest.createProperty(MANIFEST_NS + "loadOrder");
            Property sourceFileProp = manifest.createProperty(MANIFEST_NS + "sourceFile");
            Resource partitionClass = manifest.createResource(MANIFEST_NS + "OntologyPartition");

            List<Map.Entry<Integer, String>> partitions = new ArrayList<>();
            ResIterator it = manifest.listSubjectsWithProperty(RDF.type, partitionClass);
            while (it.hasNext()) {
                Resource partition = it.next();
                if (!partition.hasProperty(loadOrderProp) || !partition.hasProperty(sourceFileProp))
                    continue;
                int order   = partition.getProperty(loadOrderProp).getInt();
                String file = partition.getProperty(sourceFileProp).getString();
                partitions.add(Map.entry(order, file));
            }
            partitions.sort(Comparator.comparingInt(Map.Entry::getKey));

            String baseUrl  = MANIFEST_URL.substring(0, MANIFEST_URL.lastIndexOf('/') + 1);
            String basePath = System.getProperty("user.dir") + "/";

            for (Map.Entry<Integer, String> entry : partitions) {
                String file = entry.getValue();
                Model partitionModel = JenaUtil.createMemoryModel();

                if (file.equals("lexical.ttl")) {
                    // Cached in Docker image — fast local read
                    String localPath = basePath + file;
                    System.out.println("[startup] Loading cached partition (order=" + entry.getKey() + "): " + localPath);
                    partitionModel.read(localPath);
                } else {
                    // Fetch live from GitHub — picks up changes on every redeploy
                    String partitionUrl = baseUrl + file;
                    System.out.println("[startup] Loading live partition (order=" + entry.getKey() + "): " + partitionUrl);
                    partitionModel = fetchTTL(partitionUrl);
                }

                merged.add(partitionModel);
                System.out.println("[startup]   -> " + partitionModel.size() + " triples (running total: " + merged.size() + ")");
            }

            System.out.println("[startup] Federated graph loaded: " + merged.size() + " total triples.");

        } catch (Exception e) {
            System.err.println("[startup] FAILED to load from manifest: " + e.getMessage());
            e.printStackTrace();
            System.err.println("[startup] Falling back to local roles_shacl.ttl...");
            try {
                merged.read("roles_shacl.ttl");
                System.out.println("[startup] Fallback loaded: " + merged.size() + " triples.");
            } catch (Exception fallbackEx) {
                System.err.println("[startup] Fallback also failed: " + fallbackEx.getMessage());
            }
        }
        return merged;
    }

    private static Model fetchTTL(String url) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Accept", "text/turtle, application/x-turtle")
            .GET()
            .build();
        HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() != 200) {
            throw new RuntimeException("HTTP " + response.statusCode() + " fetching: " + url);
        }
        Model model = JenaUtil.createMemoryModel();
        model.read(response.body(), url, "TTL");
        return model;
    }

    private static void lookupVerb(Context ctx) {
        String query = ctx.queryParam("verb");
        if (query == null || query.isBlank()) {
            ctx.json(Map.of("found", false));
            return;
        }

        String searchStem = query.trim().toLowerCase();
        List<Map<String, Object>> sensesResult = new ArrayList<>();

        Property senseProp  = SHAPES_GRAPH.createProperty(ONT_NS + "sense");
        Property evokesProp = SHAPES_GRAPH.createProperty(ONT_NS + "evokes");
        Property glossProp  = SHAPES_GRAPH.createProperty(ONT_NS + "gloss");
        Resource lemmaClass = SHAPES_GRAPH.createResource(ONT_NS + "Lemma");

        ResIterator lemmas = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, lemmaClass);
        while (lemmas.hasNext()) {
            Resource lemma = lemmas.next();
            if (!lemma.hasProperty(RDFS.label)) continue;

            String label = lemma.getProperty(RDFS.label).getString().toLowerCase();
            String stem = label.contains(" (") ? label.substring(0, label.indexOf(" (")).trim() : label;

            if (stem.equals(searchStem)) {
                StmtIterator senseStmts = lemma.listProperties(senseProp);
                while (senseStmts.hasNext()) {
                    Resource synset = senseStmts.next().getResource();
                    String synsetId = synset.getLocalName();

                    String gloss = synset.hasProperty(glossProp)
                            ? synset.getProperty(glossProp).getString()
                            : "No definition available.";

                    List<String> situations = new ArrayList<>();
                    StmtIterator evokedStmts = synset.listProperties(evokesProp);
                    while (evokedStmts.hasNext()) {
                        Resource shape = evokedStmts.next().getResource();
                        situations.add(shape.getLocalName());
                    }

                    if (!situations.isEmpty()) {
                        sensesResult.add(Map.of(
                                "id", synsetId,
                                "gloss", gloss,
                                "situations", situations
                        ));
                    }
                }
                break;
            }
        }

        ctx.json(Map.of(
                "found", !sensesResult.isEmpty(),
                "lemma", searchStem,
                "senses", sensesResult
        ));
    }

    private static void getForms(Context ctx) {
        Map<String, Object> forms = new HashMap<>();

        ResIterator shapes = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape);
        while (shapes.hasNext()) {
            Resource shape = shapes.next();
            String shapeName = shape.getLocalName();
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
                forms.put(shapeName, Map.of("fields", fields));
            }
        }

        ctx.json(Map.of("forms", forms));
    }

    private static void getStats(Context ctx) {
        long shapeCount = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).toList().size();

        Resource protoRoleRoot = SHAPES_GRAPH.getResource(ONT_NS + "proto_role");
        long roleCount = countDescendants(protoRoleRoot);

        Resource modifierRoot = SHAPES_GRAPH.getResource(ONT_NS + "modifier");
        long modifierCount = countDescendants(modifierRoot);

        long ruleCount = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.SPARQLRule).toList().size();
        long lemmaCount = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, 
                SHAPES_GRAPH.createResource(ONT_NS + "Lemma")).toList().size();
        long senseCount = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, 
                SHAPES_GRAPH.createResource(ONT_NS + "Synset")).toList().size();

        ctx.json(Map.of(
                "shapes", shapeCount,
                "roles", roleCount,
                "modifiers", modifierCount,
                "rules", ruleCount,
                "lemmas", lemmaCount,
                "senses", senseCount
        ));
    }

    private static long countDescendants(Resource root) {
        Set<Resource> descendants = new HashSet<>();
        collectDescendantsRecursive(root, descendants);
        return descendants.size();
    }

    private static void collectDescendantsRecursive(Resource parent, Set<Resource> visited) {
        ResIterator it = SHAPES_GRAPH.listSubjectsWithProperty(RDFS.subPropertyOf, parent);
        while (it.hasNext()) {
            Resource child = it.next();
            if (!visited.contains(child)) {
                visited.add(child);
                collectDescendantsRecursive(child, visited);
            }
        }
    }

    /**
     * INFER GRAPH - Runs SHACL SPARQL rules to generate opaque properties
     * This is separate from validation and adds lemma nodes as needed.
     */
    private static void inferGraph(Context ctx) {
        String ttlInput = ctx.body();
        Model dataModel = JenaUtil.createMemoryModel();

        try {
            // Load user input
            dataModel.read(new ByteArrayInputStream(ttlInput.getBytes(StandardCharsets.UTF_8)), null, "TTL");
            long inputTriples = dataModel.size();
            System.out.println("[infer] Loaded input: " + inputTriples + " triples");

            // Extract unique lemma strings from input
            Set<String> lemmasNeeded = new HashSet<>();
            Property lemmaProp = SHAPES_GRAPH.createProperty(ONT_NS + "lemma");
            
            StmtIterator lemmaStmts = dataModel.listStatements(null, lemmaProp, (RDFNode) null);
            while (lemmaStmts.hasNext()) {
                Statement stmt = lemmaStmts.next();
                if (stmt.getObject().isLiteral()) {
                    lemmasNeeded.add(stmt.getObject().asLiteral().getString());
                }
            }
            
            System.out.println("[infer] Found " + lemmasNeeded.size() + " unique lemmas: " + lemmasNeeded);

            // Add lemma nodes from SHAPES_GRAPH
            Property present3sgProp = SHAPES_GRAPH.createProperty(ONT_NS + "present3sg");
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
                    
                    if (lemmaNode.hasProperty(present3sgProp)) {
                        String inflection = lemmaNode.getProperty(present3sgProp).getString();
                        System.out.println("[infer]   Added: " + lemmaStr + " → " + inflection);
                    }
                }
            }
            
            System.out.println("[infer] Added " + lemmaNodesAdded + " lemma nodes");
            System.out.println("[infer] Data model before inference: " + dataModel.size() + " triples");

            // Run SHACL inference (SPARQL rules)
            System.out.println("[infer] Executing SHACL rules...");
            long beforeInference = dataModel.size();
            Model newTriples = RuleUtil.executeRules(dataModel, SHAPES_GRAPH, null, null);
            
            // Merge the new triples into the original model
            dataModel.add(newTriples);
            long inferredTriples = dataModel.size() - beforeInference;
            System.out.println("[infer] Generated " + inferredTriples + " new triples");

            // CLEANUP: Remove lemma nodes (they were only needed for inference)
            Resource lemmaClass = SHAPES_GRAPH.createResource(ONT_NS + "Lemma");
            List<Statement> lemmaStatements = new ArrayList<>();
            
            // Collect all statements where subject is a Lemma
            ResIterator lemmas = dataModel.listSubjectsWithProperty(RDF.type, lemmaClass);
            while (lemmas.hasNext()) {
                Resource lemma = lemmas.next();
                StmtIterator stmts = lemma.listProperties();
                while (stmts.hasNext()) {
                    lemmaStatements.add(stmts.next());
                }
            }
            
            // Remove them
            dataModel.remove(lemmaStatements);
            System.out.println("[infer] Removed " + lemmaStatements.size() + " lemma triples");

            // Return complete data (original + inferred, minus lemma nodes)
            // Register prefixes so output uses compact notation, not full URIs
            dataModel.setNsPrefix("",    "http://example.org/ontology/");
            dataModel.setNsPrefix("temp","http://example.org/temp/");
            dataModel.setNsPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
            dataModel.setNsPrefix("rdfs","http://www.w3.org/2000/01/rdf-schema#");
            StringWriter dataWriter = new StringWriter();
            RDFDataMgr.write(dataWriter, dataModel, RDFFormat.TURTLE_PRETTY);

            System.out.println("[infer] Complete. Total: " + dataModel.size() + " triples");

            ctx.json(Map.of(
                    "success", true,
                    "inferred_data", dataWriter.toString(),
                    "stats", Map.of(
                            "input_triples", inputTriples,
                            "lemma_nodes_added", lemmaNodesAdded,
                            "inferred_triples", inferredTriples,
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
     * Validates the input data against the shapes.
     */
    private static void validateGraph(Context ctx) {
        String ttlInput = ctx.body();
        Model dataModel = JenaUtil.createMemoryModel();

        try {
            // Load user input
            dataModel.read(new ByteArrayInputStream(ttlInput.getBytes(StandardCharsets.UTF_8)), null, "TTL");
            System.out.println("[validate] Loaded input: " + dataModel.size() + " triples");

            // Run validation (no inference)
            System.out.println("[validate] Running SHACL validation...");
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
