package com.example;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.topbraid.jenax.util.JenaUtil;
import org.topbraid.shacl.validation.ValidationUtil;
import org.topbraid.shacl.vocabulary.SH;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class App {

    private static final String ONT_NS = "http://example.org/ontology/";
    // Load the graph once at startup
    private static final Model SHAPES_GRAPH = loadShapesGraph();

    public static void main(String[] args) {
        // FIXED: Updated CORS configuration for Javalin 6+
        Javalin app = Javalin.create(config -> {
            config.bundledPlugins.enableCors(cors -> {
                cors.addRule(it -> it.anyHost());
            });
        }).start(8080);

        // API Endpoints
        app.get("/api/stats", App::getStats);
        app.get("/api/lookup", App::lookupVerb);
        app.get("/api/forms", App::getForms);
        app.post("/api/validate", App::validateGraph);

        System.out.println("Server running on port 8080");
    }

    private static Model loadShapesGraph() {
        Model m = JenaUtil.createMemoryModel();
        try {
            // Using the standardized filename pushed to origin
            m.read("roles_shacl.ttl");
            System.out.println("[startup] Loaded graph: " + m.size() + " triples.");
        } catch (Exception e) {
            System.err.println("[startup] FAILED to load graph: " + e.getMessage());
            e.printStackTrace();
        }
        return m;
    }

    /**
     * LOOKUP VERB
     * Logic: Input String -> :Lemma (via label) -> :Synset (via :sense) -> :SituationShape (via :evokes)
     */
    private static void lookupVerb(Context ctx) {
        String query = ctx.queryParam("verb");
        if (query == null || query.isBlank()) {
            ctx.json(Map.of("found", false));
            return;
        }

        String searchStem = query.trim().toLowerCase();
        List<Map<String, Object>> sensesResult = new ArrayList<>();

        // Define Vocabulary
        Property senseProp  = SHAPES_GRAPH.createProperty(ONT_NS + "sense");
        Property evokesProp = SHAPES_GRAPH.createProperty(ONT_NS + "evokes");
        Property glossProp  = SHAPES_GRAPH.createProperty(ONT_NS + "gloss");
        Resource lemmaClass = SHAPES_GRAPH.createResource(ONT_NS + "Lemma");

        // 1. Find the Lemma by matching rdfs:label
        // Labels look like: "run (verb lemma)" or "run"
        ResIterator lemmas = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, lemmaClass);
        while (lemmas.hasNext()) {
            Resource lemma = lemmas.next();
            if (!lemma.hasProperty(RDFS.label)) continue;

            String label = lemma.getProperty(RDFS.label).getString().toLowerCase();
            // Clean label: "acquire (verb lemma)" -> "acquire"
            String stem = label.contains(" (") ? label.substring(0, label.indexOf(" (")).trim() : label;

            if (stem.equals(searchStem)) {
                // 2. Lemma Found: Iterate its Senses (Synsets)
                StmtIterator senseStmts = lemma.listProperties(senseProp);
                while (senseStmts.hasNext()) {
                    Resource synset = senseStmts.next().getResource();
                    String synsetId = synset.getLocalName(); // e.g., "acquire.v.01"

                    // Get Gloss
                    String gloss = synset.hasProperty(glossProp)
                            ? synset.getProperty(glossProp).getString()
                            : "No definition available.";

                    // Get Evoked Situations (Shapes)
                    List<String> situations = new ArrayList<>();
                    StmtIterator evokedStmts = synset.listProperties(evokesProp);
                    while (evokedStmts.hasNext()) {
                        Resource shape = evokedStmts.next().getResource();
                        situations.add(shape.getLocalName()); // e.g., "Transfer_of_Possession_shape"
                    }

                    // Add to results if valid
                    if (!situations.isEmpty()) {
                        sensesResult.add(Map.of(
                                "id", synsetId,
                                "gloss", gloss,
                                "situations", situations
                        ));
                    }
                }
                // Stop after finding the first matching lemma
                break;
            }
        }

        ctx.json(Map.of(
                "found", !sensesResult.isEmpty(),
                "lemma", searchStem,
                "senses", sensesResult
        ));
    }

    /**
     * GET FORMS
     * Scans SHACL NodeShapes to build the UI form definitions.
     */
    private static void getForms(Context ctx) {
        Map<String, Object> forms = new HashMap<>();

        // Iterate all NodeShapes
        ResIterator shapes = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape);
        while (shapes.hasNext()) {
            Resource shape = shapes.next();
            String shapeName = shape.getLocalName();
            List<Map<String, Object>> fields = new ArrayList<>();

            // Iterate sh:property definitions
            StmtIterator props = shape.listProperties(SH.property);
            while (props.hasNext()) {
                Resource propertyShape = props.next().getResource();

                // Extract SHACL constraints
                String path = propertyShape.hasProperty(SH.path)
                        ? propertyShape.getPropertyResourceValue(SH.path).getLocalName()
                        : "unknown";

                String name = propertyShape.hasProperty(SH.name)
                        ? propertyShape.getProperty(SH.name).getString()
                        : path; // Fallback to path if no name

                boolean required = propertyShape.hasProperty(SH.minCount)
                        && propertyShape.getProperty(SH.minCount).getInt() > 0;

                fields.add(Map.of(
                        "label", name,
                        "path", path,
                        "required", required
                ));
            }
            
            // Only add shapes that have fields
            if (!fields.isEmpty()) {
                forms.put(shapeName, Map.of("fields", fields));
            }
        }

        ctx.json(Map.of("forms", forms));
    }

    /**
     * GET STATS
     * Counts the key classes and traverses the RDFS hierarchy to count
     * specific roles and modifiers.
     */
    private static void getStats(Context ctx) {
        long shapeCount = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).toList().size();

        // 1. Count Roles: recursively find all sub-properties of :proto_role
        Resource protoRoleRoot = SHAPES_GRAPH.getResource(ONT_NS + "proto_role");
        long roleCount = countDescendants(protoRoleRoot);

        // 2. Count Modifiers: recursively find all sub-properties of :modifier
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

    /**
     * Recursive helper to count all unique sub-properties (descendants) of a given root.
     */
    private static long countDescendants(Resource root) {
        Set<Resource> descendants = new HashSet<>();
        collectDescendantsRecursive(root, descendants);
        return descendants.size();
    }

    private static void collectDescendantsRecursive(Resource parent, Set<Resource> visited) {
        // Find all subjects where ?s rdfs:subPropertyOf parent
        ResIterator it = SHAPES_GRAPH.listSubjectsWithProperty(RDFS.subPropertyOf, parent);
        while (it.hasNext()) {
            Resource child = it.next();
            // Prevent cycles and double counting
            if (!visited.contains(child)) {
                visited.add(child);
                // Recursively find children of this child
                collectDescendantsRecursive(child, visited);
            }
        }
    }

    /**
     * VALIDATE GRAPH
     * Performs SHACL validation and rule inference on the input Turtle.
     */
    private static void validateGraph(Context ctx) {
        String ttlInput = ctx.body();
        Model dataModel = JenaUtil.createMemoryModel();

        try {
            // Load user input
            dataModel.read(new ByteArrayInputStream(ttlInput.getBytes(StandardCharsets.UTF_8)), null, "TTL");

            // Perform Validation
            Resource report = ValidationUtil.validateModel(dataModel, SHAPES_GRAPH, true);
            StringWriter reportWriter = new StringWriter();
            RDFDataMgr.write(reportWriter, report.getModel(), RDFFormat.TURTLE);

            // Return result
            boolean conforms = report.getProperty(SH.conforms).getBoolean();
            
            // Expand data (Inference)
            StringWriter dataWriter = new StringWriter();
            RDFDataMgr.write(dataWriter, dataModel, RDFFormat.TURTLE);

            ctx.json(Map.of(
                    "conforms", conforms,
                    "report_text", reportWriter.toString(),
                    "expanded_data", dataWriter.toString()
            ));

        } catch (Exception e) {
            ctx.status(400).result("Error processing Turtle: " + e.getMessage());
        }
    }
}
