package com.example;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.Lang;
import org.topbraid.shacl.rules.RuleUtil;
import org.topbraid.shacl.validation.ValidationUtil;
import org.apache.jena.util.FileUtils;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class App {

    private static final String ONT_NS   = "https://falcontologist.github.io/shacl-demo/ontology/";
    // manifest uses ONT_NS for its properties
    private static final String MANIFEST_URL =
        System.getenv().getOrDefault("ONTOLOGY_MANIFEST",
            "https://raw.githubusercontent.com/falcontologist/SHACL-API-Docker/main/manifest.ttl");

    private static final String VIRTUOSO_SPARQL =
        System.getenv().getOrDefault("VIRTUOSO_SPARQL_URL",
            "http://virtuoso:8890/sparql");

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    // Shared in-memory ontology model loaded at startup
    static Model ontologyModel;

    public static void main(String[] args) throws Exception {

        // ── Load federated ontology graph ─────────────────────────────────────
        ontologyModel = loadOntology();

        // ── Javalin app ───────────────────────────────────────────────────────
        Javalin app = Javalin.create(config -> {
            config.bundledPlugins.enableCors(cors -> {
                cors.addRule(rule -> rule.anyHost());
            });
        }).start(8080);

        System.out.println("Server running on port 8080");

        // ── Routes ────────────────────────────────────────────────────────────
        app.get("/api/status",  App::status);
        app.get("/api/stats",   App::stats);
        app.get("/api/forms",   App::forms);
        app.get("/api/lookup",  App::lookup);
        app.post("/api/infer",  App::inferGraph);
        app.post("/api/validate", App::validateGraph);
        app.post("/api/save",   SaveRoute::handle);
        app.get("/api/sparql",  App::sparqlProxy);
    }

    // ── SPARQL proxy ──────────────────────────────────────────────────────────
    static void sparqlProxy(Context ctx) throws Exception {
        String query = ctx.queryParam("query");
        if (query == null || query.isBlank()) {
            ctx.status(400).result("Missing query parameter");
            return;
        }

        String url = VIRTUOSO_SPARQL
            + "?query=" + URLEncoder.encode(query, StandardCharsets.UTF_8)
            + "&format=json";

        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Accept", "application/sparql-results+json")
            .GET()
            .build();

        HttpResponse<String> resp = HTTP.send(req, BodyHandlers.ofString());
        System.out.println("[sparql] proxy → " + resp.statusCode());

        ctx.status(resp.statusCode())
           .contentType("application/json")
           .result(resp.body());
    }

    // ── Status ────────────────────────────────────────────────────────────────
    static void status(Context ctx) {
        ctx.json(Map.of("status", "ok", "triples", ontologyModel.size()));
    }

    // ── Stats ─────────────────────────────────────────────────────────────────
    static void stats(Context ctx) {
        long shapes = ontologyModel.listSubjectsWithProperty(
            ontologyModel.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            ontologyModel.createResource(ONT_NS + "Situation_shape")).toList().size();

        long roles = ontologyModel.listSubjectsWithProperty(
            ontologyModel.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            ontologyModel.createResource(ONT_NS + "Role")).toList().size();

        long rules = ontologyModel.listSubjectsWithProperty(
            ontologyModel.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            ontologyModel.createResource("http://www.w3.org/ns/shacl#SPARQLRule")).toList().size();

        long lemmas = ontologyModel.listSubjectsWithProperty(
            ontologyModel.createProperty(ONT_NS + "lemma")).toList().size();

        long senses = ontologyModel.listSubjectsWithProperty(
            ontologyModel.createProperty(ONT_NS + "gloss")).toList().size();

        ctx.json(Map.of(
            "shapes", shapes,
            "roles",  roles,
            "rules",  rules,
            "lemmas", lemmas,
            "senses", senses,
            "total_triples", ontologyModel.size()
        ));
    }

    // ── Forms ─────────────────────────────────────────────────────────────────
    static void forms(Context ctx) {
        Map<String, Object> forms = new LinkedHashMap<>();

        ontologyModel.listSubjectsWithProperty(
            ontologyModel.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            ontologyModel.createResource(ONT_NS + "Situation_shape")
        ).forEach(shape -> {
            String shapeId = shape.getLocalName();
            List<Map<String, Object>> fields = new ArrayList<>();

            shape.listProperties(ontologyModel.createProperty("http://www.w3.org/ns/shacl#property"))
                .forEach(propStmt -> {
                    Resource prop = propStmt.getObject().asResource();
                    Statement nameSt = prop.getProperty(
                        ontologyModel.createProperty("http://www.w3.org/ns/shacl#name"));
                    Statement pathSt = prop.getProperty(
                        ontologyModel.createProperty("http://www.w3.org/ns/shacl#path"));
                    Statement minSt  = prop.getProperty(
                        ontologyModel.createProperty("http://www.w3.org/ns/shacl#minCount"));

                    if (nameSt != null && pathSt != null) {
                        fields.add(Map.of(
                            "label",    nameSt.getLiteral().getString(),
                            "path",     pathSt.getObject().toString(),
                            "required", minSt != null && minSt.getLiteral().getInt() > 0
                        ));
                    }
                });

            if (!fields.isEmpty()) {
                forms.put(shapeId, Map.of("fields", fields));
            }
        });

        ctx.json(Map.of("forms", forms));
    }

    // ── Lookup ────────────────────────────────────────────────────────────────
    static void lookup(Context ctx) {
        String verb = ctx.queryParam("verb");
        if (verb == null || verb.isBlank()) { ctx.status(400).result("Missing verb"); return; }

        Property lemmaProp  = ontologyModel.createProperty(ONT_NS + "lemma");
        Property glossProp  = ontologyModel.createProperty(ONT_NS + "gloss");
        Property sitProp    = ontologyModel.createProperty(ONT_NS + "situation");

        List<Map<String, Object>> senses = new ArrayList<>();

        ontologyModel.listSubjectsWithProperty(lemmaProp, verb).forEach(lemmaNode -> {
            ontologyModel.listSubjectsWithProperty(
                ontologyModel.createProperty(ONT_NS + "sense"), lemmaNode
            ).forEach(synsetNode -> {
                Statement glossSt = synsetNode.getProperty(glossProp);
                if (glossSt == null) return;

                List<String> situations = new ArrayList<>();
                synsetNode.listProperties(sitProp).forEach(s ->
                    situations.add(s.getObject().asResource().getLocalName()));

                senses.add(Map.of(
                    "id",         synsetNode.getLocalName(),
                    "gloss",      glossSt.getLiteral().getString(),
                    "situations", situations
                ));
            });
        });

        ctx.json(Map.of("found", !senses.isEmpty(), "senses", senses));
    }

    // ── Infer ─────────────────────────────────────────────────────────────────
    static void inferGraph(Context ctx) throws Exception {
        String turtle = ctx.body();
        if (turtle == null || turtle.isBlank()) { ctx.status(400).result("Empty body"); return; }

        Model inputModel = ModelFactory.createDefaultModel();
        try (InputStream is = new ByteArrayInputStream(turtle.getBytes(StandardCharsets.UTF_8))) {
            RDFDataMgr.read(inputModel, is, Lang.TURTLE);
        } catch (Exception e) {
            ctx.status(400).result("Invalid Turtle: " + e.getMessage());
            return;
        }

        System.out.println("[infer] Loaded input: " + inputModel.size() + " triples");

        // Add lemma nodes from ontology for present3sg lookup
        Property lemmaProp = ontologyModel.createProperty(ONT_NS + "lemma");
        Property p3sgProp  = ontologyModel.createProperty(ONT_NS + "present3sg");

        Set<String> lemmas = new HashSet<>();
        inputModel.listObjectsOfProperty(lemmaProp).forEach(o -> {
            if (o.isLiteral()) lemmas.add(o.asLiteral().getString());
        });

        System.out.println("[infer] Found " + lemmas.size() + " unique lemmas: " + lemmas);

        Model lemmaModel = ModelFactory.createDefaultModel();
        lemmas.forEach(lemma -> {
            ontologyModel.listSubjectsWithProperty(lemmaProp, lemma).forEach(lemmaNode -> {
                Statement p3sg = lemmaNode.getProperty(p3sgProp);
                if (p3sg != null) {
                    Resource ln = lemmaModel.createResource(lemmaNode.getURI());
                    ln.addProperty(lemmaModel.createProperty(ONT_NS + "lemma"), lemma);
                    ln.addProperty(lemmaModel.createProperty(ONT_NS + "present3sg"),
                        p3sg.getLiteral().getString());
                    System.out.println("[infer]   Added: " + lemma + " → " + p3sg.getLiteral().getString());
                }
            });
        });

        System.out.println("[infer] Added " + lemmas.size() + " lemma nodes");

        Model dataModel = ModelFactory.createDefaultModel();
        dataModel.add(inputModel);
        dataModel.add(lemmaModel);
        dataModel.setNsPrefix("",     ONT_NS);
        dataModel.setNsPrefix("temp", "https://falcontologist.github.io/shacl-demo/temp/");
        dataModel.setNsPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        dataModel.setNsPrefix("rdf",  "http://www.w3.org/1999/02/22-rdf-syntax-ns#");

        System.out.println("[infer] Data model before inference: " + dataModel.size() + " triples");

        Model shapesModel = ontologyModel;
        System.out.println("[infer] Executing SHACL rules...");

        Model inferredModel;
        try {
            inferredModel = RuleUtil.executeRules(dataModel, shapesModel, null, null);
        } catch (Exception e) {
            System.err.println("[infer] Rule execution error: " + e.getMessage());
            ctx.status(500).result("Inference error: " + e.getMessage());
            return;
        }
        long newTriples = inferredModel.size();
        System.out.println("[infer] Generated " + newTriples + " new triples");

        // Remove lemma/synset triples (opaque — not for display)
        dataModel.removeAll(null, dataModel.createProperty(ONT_NS + "lemma"), null);
        dataModel.removeAll(null, dataModel.createProperty(ONT_NS + "synset"), null);
        dataModel.removeAll(null, dataModel.createProperty(ONT_NS + "present3sg"), null);
        dataModel.remove(lemmaModel);

        long removedTriples = inputModel.size() - dataModel.size();
        System.out.println("[infer] Removed " + removedTriples + " lemma triples");

        dataModel.add(inferredModel);
        System.out.println("[infer] Complete. Total: " + dataModel.size() + " triples");

        StringWriter sw = new StringWriter();
        dataModel.write(sw, "TURTLE");

        ctx.json(Map.of(
            "success", true,
            "inferred_data", sw.toString(),
            "stats", Map.of(
                "input_triples",    inputModel.size(),
                "inferred_triples", newTriples,
                "total_triples",    dataModel.size()
            )
        ));
    }

    // ── Validate ──────────────────────────────────────────────────────────────
    static void validateGraph(Context ctx) throws Exception {
        String turtle = ctx.body();
        if (turtle == null || turtle.isBlank()) { ctx.status(400).result("Empty body"); return; }

        Model dataModel = ModelFactory.createDefaultModel();
        try (InputStream is = new ByteArrayInputStream(turtle.getBytes(StandardCharsets.UTF_8))) {
            RDFDataMgr.read(dataModel, is, Lang.TURTLE);
        } catch (Exception e) {
            ctx.status(400).result("Invalid Turtle: " + e.getMessage());
            return;
        }

        Resource report = ValidationUtil.validateModel(dataModel, ontologyModel, true);
        boolean conforms = report.getProperty(
            report.getModel().createProperty("http://www.w3.org/ns/shacl#conforms")
        ).getBoolean();

        StringWriter sw = new StringWriter();
        report.getModel().write(sw, "TURTLE");

        ctx.json(Map.of(
            "conforms",     conforms,
            "report_text",  sw.toString()
        ));
    }

    // ── Load ontology ─────────────────────────────────────────────────────────
    static Model loadOntology() throws Exception {
        System.out.println("[startup] Loading manifest from: " + MANIFEST_URL);

        // Derive base URL from manifest URL (everything up to and including last /)
        String baseUrl = MANIFEST_URL.substring(0, MANIFEST_URL.lastIndexOf('/') + 1);

        Model manifest = ModelFactory.createDefaultModel();
        RDFDataMgr.read(manifest, MANIFEST_URL);
        System.out.println("[startup] Manifest loaded: " + manifest.size() + " triples");

        // Print all distinct namespaces seen in the manifest for debugging
        manifest.listStatements().forEach(stmt -> {
            String ns = stmt.getPredicate().getNameSpace();
            System.out.println("[startup] predicate ns: " + ns + " | local: " + stmt.getPredicate().getLocalName());
        });

        // Manifest schema uses :sourceFile and :loadOrder under ONT_NS
        Property orderProp  = manifest.createProperty(ONT_NS + "loadOrder");
        Property sourceProp = manifest.createProperty(ONT_NS + "sourceFile");
        System.out.println("[startup] Looking for orderProp: " + orderProp);

        // Find all partitions that have a loadOrder
        List<Resource> partitions = manifest
            .listSubjectsWithProperty(orderProp).toList();
        System.out.println("[startup] Found " + partitions.size() + " partitions");

        if (partitions.isEmpty()) {
            System.err.println("[startup] WARNING: No partitions found in manifest. " +
                "Check that manifest uses <" + ONT_NS + "loadOrder> and <" + ONT_NS + "sourceFile>.");
        }

        partitions.sort(Comparator.comparingInt(r ->
            r.getProperty(orderProp).getInt()));

        Model combined = ModelFactory.createDefaultModel();
        long running = 0;

        for (Resource partition : partitions) {
            Statement srcSt = partition.getProperty(sourceProp);
            if (srcSt == null) {
                System.out.println("[startup] Skipping partition with no sourceFile: " + partition);
                continue;
            }
            int order = partition.getProperty(orderProp).getInt();
            String sourceFile = srcSt.getLiteral().getString();
            String url = baseUrl + sourceFile;

            System.out.println("[startup] Loading partition " + order + ": " + url);
            RDFDataMgr.read(combined, url);

            long added = combined.size() - running;
            running = combined.size();
            System.out.println("[startup]   -> +" + added + " triples (total: " + running + ")");
        }

        System.out.println("[startup] Federated graph loaded: " + combined.size() + " total triples.");
        return combined;
    }
}
