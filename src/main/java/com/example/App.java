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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class App {

    private static final String ONT_NS   = "https://falcontologist.github.io/shacl-demo/ontology/";

    private static final String MANIFEST_URL =
        System.getenv().getOrDefault("ONTOLOGY_MANIFEST",
            "https://raw.githubusercontent.com/falcontologist/SHACL-API-Docker/main/manifest.ttl");

    // Virtuoso SPARQL endpoint (read-only)
    private static final String VIRTUOSO_SPARQL =
        System.getenv().getOrDefault("VIRTUOSO_SPARQL_URL",
            "https://fkg-6htt.onrender.com/sparql");

    // Virtuoso graph IRI
    private static final String VIRTUOSO_GRAPH =
        System.getenv().getOrDefault("VIRTUOSO_GRAPH_IRI",
            "http://shacl-demo.org/type");

    // API key for protected endpoints (should be set in Render environment)
    private static final String LOAD_API_KEY =
        System.getenv().getOrDefault("LOAD_API_KEY", "default-change-me");

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    // Small ontology model (conceptual + structural + lexical) for SHACL inference
    static Model ontologyModel;

    // Entity autocomplete: Lucene FST backed, data from Virtuoso
    static EntitySuggestService entitySuggestService;

    public static void main(String[] args) throws Exception {

        // ── Load small ontology into Jena (fast) ────────────────────────────
        ontologyModel = loadOntology();

        // ── Initialize entity suggest service ───────────────────────────────
        entitySuggestService = new EntitySuggestService(VIRTUOSO_SPARQL, VIRTUOSO_GRAPH);

        // ── Start server immediately (FST builds in background) ─────────────
        Javalin app = Javalin.create(config -> {
            config.bundledPlugins.enableCors(cors -> {
                cors.addRule(rule -> rule.anyHost());
            });
        }).start(8080);

        System.out.println("Server running on port 8080");

        // ── Routes ────────────────────────────────────────────────────────────
        app.get("/api/status",          App::status);
        app.get("/api/stats",           App::stats);
        app.get("/api/forms",           App::forms);
        app.get("/api/lookup",          App::lookup);
        app.post("/api/infer",          App::inferGraph);
        app.post("/api/validate",       App::validateGraph);
        app.post("/api/save",           SaveRoute::handle);
        app.get("/api/sparql",          App::sparqlProxy);

        // Entity autocomplete (Lucene FST + Virtuoso)
        app.get("/api/entity-suggest",  App::entitySuggest);
        app.get("/api/entity-senses",   App::entitySenses);
        app.get("/api/entity-stats",    App::entityStats);

        // New endpoint to load all partitions from manifest.ttl into Virtuoso
        app.post("/api/load-virtuoso-from-manifest", App::loadVirtuosoFromManifest);

        // ── Load pre-built FST index in background thread ────────────────
        // Server is already accepting requests; suggest returns empty until ready.
        Thread indexThread = new Thread(() -> {
            try {
                entitySuggestService.loadPrebuiltIndex();
            } catch (Exception e) {
                System.err.println("[startup] FST index load failed: " + e.getMessage());
                e.printStackTrace();
            }
        }, "entity-suggest-loader");
        indexThread.setDaemon(true);
        indexThread.start();
    }

    // ── Dynamic Virtuoso loader using manifest.ttl ───────────────────────────
    static void loadVirtuosoFromManifest(Context ctx) {
        String providedKey = ctx.header("X-API-Key");
        if (!LOAD_API_KEY.equals(providedKey)) {
            ctx.status(403).json(Map.of("error", "Unauthorized"));
            return;
        }

        try {
            // 1. Load manifest from local file (copied by Dockerfile)
            String manifestPath = "/app/manifest.ttl";
            if (!Files.exists(Paths.get(manifestPath))) {
                ctx.status(500).json(Map.of("error", "manifest.ttl not found in container at " + manifestPath));
                return;
            }

            Model manifest = ModelFactory.createDefaultModel();
            try (FileInputStream fis = new FileInputStream(manifestPath)) {
                RDFDataMgr.read(manifest, fis, Lang.TURTLE);
            }

            // 2. Define properties
            Property loadOrderProp = manifest.createProperty(ONT_NS + "loadOrder");
            Property sourceFileProp = manifest.createProperty(ONT_NS + "sourceFile");

            // 3. Collect partitions with loadOrder
            List<Resource> partitions = manifest.listSubjectsWithProperty(loadOrderProp).toList();
            if (partitions.isEmpty()) {
                ctx.status(500).json(Map.of("error", "No partitions with loadOrder found in manifest"));
                return;
            }

            // 4. Sort by loadOrder
            partitions.sort(Comparator.comparingInt(r -> r.getProperty(loadOrderProp).getInt()));

            // 5. Load each file into Virtuoso
            List<Map<String, Object>> results = new ArrayList<>();
            int successCount = 0;

            for (Resource partition : partitions) {
                Statement srcSt = partition.getProperty(sourceFileProp);
                if (srcSt == null) {
                    results.add(Map.of("partition", partition.getLocalName(), "status", "skipped (no sourceFile)"));
                    continue;
                }

                String sourceFile = srcSt.getLiteral().getString();
                String filePath = "/app/" + sourceFile; // files are in the container root (from Dockerfile COPY)
                if (!Files.exists(Paths.get(filePath))) {
                    results.add(Map.of("file", sourceFile, "status", "skipped (file not found at " + filePath + ")"));
                    continue;
                }

                // SPARQL LOAD (using Virtuoso's LOAD <file://...>)
                String loadQuery = String.format(
                    "LOAD <file://%s> INTO GRAPH <%s>",
                    filePath, VIRTUOSO_GRAPH
                );

                String url = VIRTUOSO_SPARQL + "?query=" +
                    URLEncoder.encode(loadQuery, StandardCharsets.UTF_8);

                HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/sparql-update")
                    .POST(HttpRequest.BodyPublishers.ofString(loadQuery))
                    .build();

                HttpResponse<String> resp = HTTP.send(req, BodyHandlers.ofString());
                boolean ok = resp.statusCode() == 200;

                results.add(Map.of(
                    "file", sourceFile,
                    "order", partition.getProperty(loadOrderProp).getInt(),
                    "status", ok ? "loaded" : "failed",
                    "responseCode", resp.statusCode()
                ));
                if (ok) successCount++;

                // Small delay to avoid overwhelming Virtuoso
                Thread.sleep(1000);
            }

            // 6. Optionally trigger FST rebuild after loading
            new Thread(() -> {
                try {
                    System.out.println("[load] Data loaded, rebuilding FST...");
                    entitySuggestService.loadPrebuiltIndex();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();

            ctx.json(Map.of(
                "message", "Loading completed from manifest",
                "successCount", successCount,
                "totalPartitions", partitions.size(),
                "details", results,
                "fstRebuildStarted", true
            ));

        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", e.getMessage()));
        }
    }

    // ── Entity suggest (autocomplete — hits Lucene FST, sub-10ms) ─────────────
    static void entitySuggest(Context ctx) {
        String category = ctx.queryParam("type");
        String query = ctx.queryParam("q");
        int limit = 10;
        try {
            String limitParam = ctx.queryParam("limit");
            if (limitParam != null) limit = Integer.parseInt(limitParam);
        } catch (NumberFormatException ignored) {}

        if (category == null || category.isBlank()) {
            ctx.status(400).json(Map.of("error", "Missing 'type' parameter"));
            return;
        }
        if (query == null || query.isBlank()) {
            ctx.status(400).json(Map.of("error", "Missing 'q' parameter"));
            return;
        }

        // Resolve friendly short names
        if (!EntitySuggestService.CATEGORIES.contains(category)) {
            String resolved = switch (category.toLowerCase()) {
                case "person" -> "Person_Entity";
                case "organization", "org" -> "Organization_Entity";
                case "geopoliticalentity", "geopolitical", "gpe" -> "Geopolitical_Entity";
                case "product" -> "Product_Entity";
                case "unit" -> "Unit_Entity";
                case "occupation" -> "Occupation_Entity";
                default -> null;
            };
            if (resolved != null) {
                category = resolved;
            } else {
                ctx.status(400).json(Map.of(
                    "error", "Invalid category. Must be one of: " + EntitySuggestService.CATEGORIES
                ));
                return;
            }
        }

        try {
            long start = System.nanoTime();
            List<Map<String, String>> results = entitySuggestService.suggest(category, query, limit);
            long elapsedMicros = (System.nanoTime() - start) / 1000;

            ctx.json(Map.of(
                "results", results,
                "count", results.size(),
                "query", query,
                "type", category,
                "latencyMicros", elapsedMicros,
                "ready", entitySuggestService.isReady()
            ));
        } catch (Exception e) {
            System.err.println("[entity-suggest] Error: " + e.getMessage());
            ctx.status(500).json(Map.of("error", "Suggest failed: " + e.getMessage()));
        }
    }

    // ── Entity senses ──────────────────────────────────────────────────────────
    static void entitySenses(Context ctx) {
        String iri = ctx.queryParam("iri");
        if (iri == null || iri.isBlank()) {
            ctx.status(400).json(Map.of("error", "Missing 'iri' parameter"));
            return;
        }

        try {
            List<Map<String, String>> senses = entitySuggestService.getSenses(iri);
            ctx.json(Map.of("iri", iri, "senses", senses, "count", senses.size()));
        } catch (Exception e) {
            System.err.println("[entity-senses] Error: " + e.getMessage());
            ctx.status(500).json(Map.of("error", "Sense lookup failed: " + e.getMessage()));
        }
    }

    // ── Entity stats ───────────────────────────────────────────────────────────
    static void entityStats(Context ctx) {
        ctx.json(entitySuggestService.getStats());
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
        Map<String, Object> statusMap = new LinkedHashMap<>();
        statusMap.put("status", "ok");
        statusMap.put("triples", ontologyModel.size());
        statusMap.put("entitySuggestReady", entitySuggestService != null && entitySuggestService.isReady());
        ctx.json(statusMap);
    }

    // ── Stats ─────────────────────────────────────────────────────────────────
    static void stats(Context ctx) {
        Property typeProp  = ontologyModel.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        Property subPropOf = ontologyModel.createProperty("http://www.w3.org/2000/01/rdf-schema#subPropertyOf");

        long shapes = ontologyModel.listSubjectsWithProperty(
            typeProp,
            ontologyModel.createResource(ONT_NS + "Situation_shape")
        ).toList().size();

        long rules = ontologyModel.listSubjectsWithProperty(
            typeProp,
            ontologyModel.createResource("http://www.w3.org/ns/shacl#SPARQLRule")
        ).toList().size();

        Set<Resource> rolesSet = new HashSet<>();
        Queue<Resource> queue = new LinkedList<>();
        queue.add(ontologyModel.createResource(ONT_NS + "shape_element"));
        while (!queue.isEmpty()) {
            Resource current = queue.poll();
            ontologyModel.listSubjectsWithProperty(subPropOf, current).forEachRemaining(child -> {
                if (rolesSet.add(child)) queue.add(child);
            });
        }

        Set<Resource> lemmasSet = new HashSet<>();
        ontologyModel.listSubjectsWithProperty(ontologyModel.createProperty(ONT_NS + "lemma"))
            .forEachRemaining(lemmasSet::add);

        Set<Resource> sensesSet = new HashSet<>();
        ontologyModel.listSubjectsWithProperty(ontologyModel.createProperty(ONT_NS + "gloss"))
            .forEachRemaining(sensesSet::add);
        ontologyModel.listSubjectsWithProperty(typeProp, ontologyModel.createResource(ONT_NS + "Synset"))
            .forEachRemaining(sensesSet::add);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("shapes", shapes);
        result.put("roles", rolesSet.size());
        result.put("rules", rules);
        result.put("lemmas", lemmasSet.size());
        result.put("senses", sensesSet.size());
        result.put("total_triples", ontologyModel.size());

        if (entitySuggestService != null && entitySuggestService.isReady()) {
            result.put("entities", entitySuggestService.getStats().get("entityCounts"));
        }

        ctx.json(result);
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
        Property sitProp    = ontologyModel.createProperty(ONT_NS + "evokes");

        List<Map<String, Object>> senses = new ArrayList<>();

        ontologyModel.listSubjectsWithProperty(lemmaProp, verb).forEach(lemmaNode -> {
            lemmaNode.listProperties(ontologyModel.createProperty(ONT_NS + "sense")).forEach(senseStmt -> {
                Resource synsetNode = senseStmt.getObject().asResource();
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

        Property lemmaProp = ontologyModel.createProperty(ONT_NS + "lemma");
        Property p3sgProp  = ontologyModel.createProperty(ONT_NS + "present3sg");

        Set<String> lemmas = new HashSet<>();
        inputModel.listObjectsOfProperty(lemmaProp).forEach(o -> {
            if (o.isLiteral()) lemmas.add(o.asLiteral().getString());
        });

        Model lemmaModel = ModelFactory.createDefaultModel();
        lemmas.forEach(lemma -> {
            ontologyModel.listSubjectsWithProperty(lemmaProp, lemma).forEach(lemmaNode -> {
                Statement p3sg = lemmaNode.getProperty(p3sgProp);
                if (p3sg != null) {
                    Resource ln = lemmaModel.createResource(lemmaNode.getURI());
                    ln.addProperty(lemmaModel.createProperty(ONT_NS + "lemma"), lemma);
                    ln.addProperty(lemmaModel.createProperty(ONT_NS + "present3sg"),
                        p3sg.getLiteral().getString());
                }
            });
        });

        Model dataModel = ModelFactory.createDefaultModel();
        dataModel.add(inputModel);
        dataModel.add(lemmaModel);
        dataModel.setNsPrefix("",     ONT_NS);
        dataModel.setNsPrefix("temp", "https://falcontologist.github.io/shacl-demo/temp/");
        dataModel.setNsPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        dataModel.setNsPrefix("rdf",  "http://www.w3.org/1999/02/22-rdf-syntax-ns#");

        Model inferredModel;
        try {
            inferredModel = RuleUtil.executeRules(dataModel, ontologyModel, null, null);
        } catch (Exception e) {
            ctx.status(500).result("Inference error: " + e.getMessage());
            return;
        }

        dataModel.removeAll(null, dataModel.createProperty(ONT_NS + "lemma"), null);
        dataModel.removeAll(null, dataModel.createProperty(ONT_NS + "synset"), null);
        dataModel.removeAll(null, dataModel.createProperty(ONT_NS + "present3sg"), null);
        dataModel.remove(lemmaModel);
        dataModel.add(inferredModel);

        StringWriter sw = new StringWriter();
        dataModel.write(sw, "TURTLE");

        ctx.json(Map.of(
            "success", true,
            "inferred_data", sw.toString(),
            "stats", Map.of(
                "input_triples",    inputModel.size(),
                "inferred_triples", inferredModel.size(),
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

    static Model loadOntology() throws Exception {
        // Load structural.ttl (SHACL shapes), conceptual.ttl (synsets + evokes),
        // and lexical.ttl (lemmas + senses) — all needed for the verb→sense→situation flow.
        // Entity data (persons, orgs, etc.) stays in Virtuoso.
        String[] ontologyFiles = {
            "https://raw.githubusercontent.com/falcontologist/SHACL-API-Docker/main/structural.ttl",
            "https://raw.githubusercontent.com/falcontologist/SHACL-API-Docker/main/conceptual.ttl",
            "https://raw.githubusercontent.com/falcontologist/SHACL-API-Docker/main/lexical.ttl"
        };

        Model model = ModelFactory.createDefaultModel();

        for (String url : ontologyFiles) {
            System.out.println("[startup] Loading: " + url);
            try {
                RDFDataMgr.read(model, url);
            } catch (Exception e) {
                System.err.println("[startup] Warning: Could not load " + url + " — " + e.getMessage());
            }
        }

        System.out.println("[startup] Ontology loaded: " + model.size() + " triples.");
        return model;
    }
}