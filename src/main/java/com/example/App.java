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
import java.util.stream.Collectors;

public class App {

    private static final String ONT_NS   = "https://falcontologist.github.io/shacl-demo/ontology/";
    private static final String SH_NS    = "http://www.w3.org/ns/shacl#";
    private static final String DASH_NS  = "http://datashapes.org/dash#";
    private static final String XSD_NS   = "http://www.w3.org/2001/XMLSchema#";
    private static final String SKOS_NS  = "http://www.w3.org/2004/02/skos/core#";
    private static final String RDFS_NS  = "http://www.w3.org/2000/01/rdf-schema#";
    private static final String RDF_NS   = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    private static final String MANIFEST_URL =
        System.getenv().getOrDefault("ONTOLOGY_MANIFEST",
            "https://raw.githubusercontent.com/falcontologist/SHACL-API-Docker/main/manifest.ttl");

    // Virtuoso SPARQL endpoint (read-only)
    private static final String VIRTUOSO_SPARQL =
        System.getenv().getOrDefault("VIRTUOSO_SPARQL_URL",
            "https://fkg-6htt.onrender.com/sparql");

    // Virtuoso graph IRIs
    private static final String VIRTUOSO_GRAPH = System.getenv().getOrDefault("VIRTUOSO_GRAPH_IRI", "http://shacl-demo.org/type");
    private static final String CONCEPTUAL_GRAPH = System.getenv().getOrDefault("VIRTUOSO_CONCEPTUAL_GRAPH", "http://shacl-demo.org/conceptual");
    private static final String TOKEN_GRAPH = System.getenv().getOrDefault("VIRTUOSO_INSTANCE_GRAPH", "http://shacl-demo.org/token");

    // API key for protected endpoints
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
            String manifestPath = "/app/manifest.ttl";
            if (!Files.exists(Paths.get(manifestPath))) {
                ctx.status(500).json(Map.of("error", "manifest.ttl not found in container at " + manifestPath));
                return;
            }

            Model manifest = ModelFactory.createDefaultModel();
            try (FileInputStream fis = new FileInputStream(manifestPath)) {
                RDFDataMgr.read(manifest, fis, Lang.TURTLE);
            }

            Property loadOrderProp = manifest.createProperty(ONT_NS + "loadOrder");
            Property sourceFileProp = manifest.createProperty(ONT_NS + "sourceFile");

            List<Resource> partitions = manifest.listSubjectsWithProperty(loadOrderProp).toList();
            if (partitions.isEmpty()) {
                ctx.status(500).json(Map.of("error", "No partitions with loadOrder found in manifest"));
                return;
            }

            partitions.sort(Comparator.comparingInt(r -> r.getProperty(loadOrderProp).getInt()));

            List<Map<String, Object>> results = new ArrayList<>();
            int successCount = 0;

            for (Resource partition : partitions) {
                Statement srcSt = partition.getProperty(sourceFileProp);
                if (srcSt == null) {
                    results.add(Map.of("partition", partition.getLocalName(), "status", "skipped (no sourceFile)"));
                    continue;
                }

                String sourceFile = srcSt.getLiteral().getString();
                String filePath = "/app/" + sourceFile;
                if (!Files.exists(Paths.get(filePath))) {
                    results.add(Map.of("file", sourceFile, "status", "skipped (file not found at " + filePath + ")"));
                    continue;
                }

                String loadQuery = String.format(
                    "LOAD <file://%s> INTO GRAPH <%s>",
                    filePath, VIRTUOSO_GRAPH
                );

                String url = VIRTUOSO_SPARQL + "?query=" + URLEncoder.encode(loadQuery, StandardCharsets.UTF_8) + "&format=json";

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

                Thread.sleep(1000);
            }

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
    // Supports:
    //   ?type=all                           → search all categories
    //   ?type=Person_Entity                  → single category
    //   ?type=Person_Entity,Location_Entity  → multiple categories (comma-separated)
    //   ?role=Acquirer_Role_Player           → (optional) role class for score boosting
    static void entitySuggest(Context ctx) {
        String typeParam = ctx.queryParam("type");
        String query = ctx.queryParam("q");
        String roleClass = ctx.queryParam("role");  // Optional: for future score boosting
        int limit = 10;
        try {
            String limitParam = ctx.queryParam("limit");
            if (limitParam != null) limit = Integer.parseInt(limitParam);
        } catch (NumberFormatException ignored) {}

        if (typeParam == null || typeParam.isBlank()) {
            ctx.status(400).json(Map.of("error", "Missing 'type' parameter"));
            return;
        }
        if (query == null || query.isBlank()) {
            ctx.status(400).json(Map.of("error", "Missing 'q' parameter"));
            return;
        }

        try {
            long start = System.nanoTime();
            List<Map<String, String>> results;
            String resolvedType;

            if ("all".equalsIgnoreCase(typeParam)) {
                // All-categories mode
                results = entitySuggestService.suggestAll(query, limit);
                resolvedType = "all";
            } else if (typeParam.contains(",")) {
                // Multi-category mode: comma-separated list
                List<String> categories = new ArrayList<>();
                for (String raw : typeParam.split(",")) {
                    String resolved = resolveCategory(raw.trim());
                    if (resolved != null) categories.add(resolved);
                }
                if (categories.isEmpty()) {
                    ctx.status(400).json(Map.of("error", "No valid categories in: " + typeParam));
                    return;
                }
                results = entitySuggestService.suggestMulti(categories, query, limit);
                resolvedType = String.join(",", categories);
            } else {
                // Single category mode (original behavior)
                String resolved = resolveCategory(typeParam);
                if (resolved == null) {
                    ctx.status(400).json(Map.of(
                        "error", "Invalid category. Must be one of: all, " + EntitySuggestService.CATEGORIES
                    ));
                    return;
                }
                results = entitySuggestService.suggest(resolved, query, limit);
                resolvedType = resolved;
            }

            long elapsedMicros = (System.nanoTime() - start) / 1000;

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("results", results);
            response.put("count", results.size());
            response.put("query", query);
            response.put("type", resolvedType);
            response.put("latencyMicros", elapsedMicros);
            response.put("ready", entitySuggestService.isReady());
            if (roleClass != null && !roleClass.isBlank()) {
                response.put("roleClass", roleClass);
            }

            ctx.json(response);
        } catch (Exception e) {
            System.err.println("[entity-suggest] Error: " + e.getMessage());
            ctx.status(500).json(Map.of("error", "Suggest failed: " + e.getMessage()));
        }
    }

    /**
     * Resolve a category name to a canonical CATEGORIES entry.
     * Returns null if not recognized.
     */
    private static String resolveCategory(String category) {
        if (EntitySuggestService.CATEGORIES.contains(category)) return category;
        return switch (category.toLowerCase()) {
            case "person" -> "Person_Entity";
            case "organization", "org" -> "Organization_Entity";
            case "geopoliticalentity", "geopolitical", "gpe" -> "Geopolitical_Entity";
            case "product" -> "Product_Entity";
            case "unit" -> "Unit_Entity";
            case "occupation" -> "Occupation_Entity";
            case "creative_work", "creativework" -> "Creative_Work_Entity";
            case "quantity_dimension", "quantitydimension", "dimension" -> "Quantity_Dimension_Entity";
            case "location" -> "Location_Entity";
            case "food" -> "Food_Entity";
            case "language" -> "Language_Entity";
            case "organism" -> "Organism_Entity";
            case "equity" -> "Equity_Entity";
            case "index" -> "Index_Entity";
            case "corporate_bond", "corporatebond" -> "Corporate_Bond_Entity";
            case "government_bond", "governmentbond" -> "Government_Bond_Entity";
            default -> null;
        };
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
        Property typeProp  = ontologyModel.createProperty(RDF_NS + "type");
        Property subPropOf = ontologyModel.createProperty(RDFS_NS + "subPropertyOf");

        long shapes = ontologyModel.listSubjectsWithProperty(
            typeProp,
            ontologyModel.createResource(ONT_NS + "Situation_shape")
        ).toList().size();

        long rules = ontologyModel.listSubjectsWithProperty(
            typeProp,
            ontologyModel.createResource(SH_NS + "SPARQLRule")
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

    // ══════════════════════════════════════════════════════════════════════════
    // DASH-COMPLIANT FORMS ENDPOINT
    // Extracts full SHACL + DASH metadata from property shapes so the frontend
    // can render widgets conforming to the DASH Forms specification.
    // ══════════════════════════════════════════════════════════════════════════
    static void forms(Context ctx) {
        Property typeProp     = ontologyModel.createProperty(RDF_NS + "type");
        Property shProperty   = ontologyModel.createProperty(SH_NS + "property");
        Property shName       = ontologyModel.createProperty(SH_NS + "name");
        Property shPath       = ontologyModel.createProperty(SH_NS + "path");
        Property shMinCount   = ontologyModel.createProperty(SH_NS + "minCount");
        Property shMaxCount   = ontologyModel.createProperty(SH_NS + "maxCount");
        Property shOrder      = ontologyModel.createProperty(SH_NS + "order");
        Property shDescription= ontologyModel.createProperty(SH_NS + "description");
        Property shNodeKind   = ontologyModel.createProperty(SH_NS + "nodeKind");
        Property shOr         = ontologyModel.createProperty(SH_NS + "or");
        Property shClass      = ontologyModel.createProperty(SH_NS + "class");
        Property shDatatype   = ontologyModel.createProperty(SH_NS + "datatype");
        Property shNode       = ontologyModel.createProperty(SH_NS + "node");
        Property shIn         = ontologyModel.createProperty(SH_NS + "in");
        Property shMinLength  = ontologyModel.createProperty(SH_NS + "minLength");
        Property shMaxLength  = ontologyModel.createProperty(SH_NS + "maxLength");
        Property shPattern    = ontologyModel.createProperty(SH_NS + "pattern");
        Property dashEditor   = ontologyModel.createProperty(DASH_NS + "editor");
        Property dashViewer   = ontologyModel.createProperty(DASH_NS + "viewer");
        Property dashSingleLine = ontologyModel.createProperty(DASH_NS + "singleLine");
        Property skosExample  = ontologyModel.createProperty(SKOS_NS + "example");
        Property rdfsLabel    = ontologyModel.createProperty(RDFS_NS + "label");
        Property shTargetClass= ontologyModel.createProperty(SH_NS + "targetClass");

        Resource situationShapeType = ontologyModel.createResource(ONT_NS + "Situation_shape");

        Map<String, Object> forms = new LinkedHashMap<>();

        ontologyModel.listSubjectsWithProperty(typeProp, situationShapeType).forEach(shape -> {
            String shapeId = shape.getLocalName();
            Map<String, Object> shapeMap = new LinkedHashMap<>();

            // ── Shape-level metadata ────────────────────────────────────────
            Statement labelSt = shape.getProperty(rdfsLabel);
            if (labelSt != null) shapeMap.put("label", getLiteralString(labelSt));

            Statement descSt = shape.getProperty(shDescription);
            if (descSt != null) shapeMap.put("description", getLiteralString(descSt));

            Statement exampleSt = shape.getProperty(skosExample);
            if (exampleSt != null) shapeMap.put("example", getLiteralString(exampleSt));

            Statement targetSt = shape.getProperty(shTargetClass);
            if (targetSt != null) shapeMap.put("targetClass", targetSt.getObject().asResource().getLocalName());

            // ── Property shapes (fields) ────────────────────────────────────
            List<Map<String, Object>> fields = new ArrayList<>();

            // Collect properties declared directly on this shape
            shape.listProperties(shProperty).forEach(propStmt -> {
                Resource prop = propStmt.getObject().asResource();
                Map<String, Object> field = extractSingleField(prop,
                    shName, shPath, shMinCount, shMaxCount, shOrder, shDescription,
                    shNodeKind, dashEditor, dashViewer, dashSingleLine, shDatatype,
                    shClass, shOr, shNode, shIn, shPattern, shMinLength, shMaxLength,
                    shProperty);
                if (field != null) fields.add(field);
            });

            // Inherit properties from the base Situation_shape (e.g. start, end)
            // unless this IS the base shape
            Resource baseShape = ontologyModel.createResource(ONT_NS + "Situation_shape");
            if (!shape.equals(baseShape)) {
                Set<String> ownPaths = new HashSet<>();
                for (Map<String, Object> f : fields) {
                    if (f.containsKey("path")) ownPaths.add((String) f.get("path"));
                }

                baseShape.listProperties(shProperty).forEach(propStmt -> {
                    Resource prop = propStmt.getObject().asResource();
                    Map<String, Object> field = extractSingleField(prop,
                        shName, shPath, shMinCount, shMaxCount, shOrder, shDescription,
                        shNodeKind, dashEditor, dashViewer, dashSingleLine, shDatatype,
                        shClass, shOr, shNode, shIn, shPattern, shMinLength, shMaxLength,
                        shProperty);
                    if (field != null && !ownPaths.contains(field.get("path"))) {
                        fields.add(field);
                    }
                });
            }

            // Sort fields by sh:order
            fields.sort((a, b) -> {
                float oa = a.containsKey("order") ? ((Number) a.get("order")).floatValue() : Float.MAX_VALUE;
                float ob = b.containsKey("order") ? ((Number) b.get("order")).floatValue() : Float.MAX_VALUE;
                return Float.compare(oa, ob);
            });

            shapeMap.put("fields", fields);

            if (!fields.isEmpty()) {
                forms.put(shapeId, shapeMap);
            }
        });

        // Also extract non-Situation shapes that may be referenced as nested (e.g. Cost_shape)
        Map<String, Object> nestedShapes = new LinkedHashMap<>();
        extractNonSituationShapes(nestedShapes, typeProp, situationShapeType,
            shProperty, shName, shPath, shMinCount, shMaxCount,
            shOrder, shDescription, shNodeKind, dashEditor, dashViewer,
            dashSingleLine, shDatatype, shClass, shOr, shNode, shIn,
            shPattern, shMinLength, shMaxLength, rdfsLabel, shDescription);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("forms", forms);
        if (!nestedShapes.isEmpty()) {
            response.put("nestedShapes", nestedShapes);
        }

        ctx.json(response);
    }

    /**
     * Extract all DASH/SHACL metadata from a single property shape resource.
     * Returns null if the property has no sh:name or sh:path.
     */
    private static Map<String, Object> extractSingleField(
            Resource prop, Property shName, Property shPath,
            Property shMinCount, Property shMaxCount, Property shOrder, Property shDescription,
            Property shNodeKind, Property dashEditor, Property dashViewer, Property dashSingleLine,
            Property shDatatype, Property shClass, Property shOr, Property shNode,
            Property shIn, Property shPattern, Property shMinLength, Property shMaxLength,
            Property shProperty) {

        Map<String, Object> field = new LinkedHashMap<>();

        Statement nameSt = prop.getProperty(shName);
        if (nameSt == null) return null;
        field.put("label", getLiteralString(nameSt));

        Statement pathSt = prop.getProperty(shPath);
        if (pathSt == null) return null;
        field.put("path", pathSt.getObject().toString());

        Statement minSt = prop.getProperty(shMinCount);
        field.put("required", minSt != null && minSt.getLiteral().getInt() > 0);
        if (minSt != null) field.put("minCount", minSt.getLiteral().getInt());

        Statement maxSt = prop.getProperty(shMaxCount);
        if (maxSt != null) field.put("maxCount", maxSt.getLiteral().getInt());

        Statement orderSt = prop.getProperty(shOrder);
        if (orderSt != null) field.put("order", orderSt.getLiteral().getFloat());

        Statement fieldDescSt = prop.getProperty(shDescription);
        if (fieldDescSt != null) field.put("description", getLiteralString(fieldDescSt));

        Statement nodeKindSt = prop.getProperty(shNodeKind);
        if (nodeKindSt != null) field.put("nodeKind", nodeKindSt.getObject().asResource().getLocalName());

        Statement editorSt = prop.getProperty(dashEditor);
        if (editorSt != null) field.put("editor", editorSt.getObject().asResource().getLocalName());

        Statement viewerSt = prop.getProperty(dashViewer);
        if (viewerSt != null) field.put("viewer", viewerSt.getObject().asResource().getLocalName());

        Statement singleLineSt = prop.getProperty(dashSingleLine);
        if (singleLineSt != null) field.put("singleLine", singleLineSt.getLiteral().getBoolean());

        Statement datatypeSt = prop.getProperty(shDatatype);
        if (datatypeSt != null) field.put("datatype", datatypeSt.getObject().asResource().getLocalName());

        Statement classSt = prop.getProperty(shClass);
        if (classSt != null) field.put("class", classSt.getObject().asResource().getLocalName());

        Statement patternSt = prop.getProperty(shPattern);
        if (patternSt != null) field.put("pattern", patternSt.getLiteral().getString());

        Statement minLenSt = prop.getProperty(shMinLength);
        if (minLenSt != null) field.put("minLength", minLenSt.getLiteral().getInt());
        Statement maxLenSt = prop.getProperty(shMaxLength);
        if (maxLenSt != null) field.put("maxLength", maxLenSt.getLiteral().getInt());

        Statement inSt = prop.getProperty(shIn);
        if (inSt != null) {
            List<String> enumValues = new ArrayList<>();
            try {
                RDFList rdfList = inSt.getObject().as(RDFList.class);
                rdfList.iterator().forEachRemaining(node -> {
                    if (node.isLiteral()) enumValues.add(node.asLiteral().getString());
                    else if (node.isResource()) enumValues.add(node.asResource().getLocalName());
                });
            } catch (Exception e) { /* not a valid list */ }
            if (!enumValues.isEmpty()) field.put("in", enumValues);
        }

        Statement orSt = prop.getProperty(shOr);
        if (orSt != null) {
            List<Map<String, Object>> orConstraints = parseShOrList(orSt.getObject(), shClass, shDatatype, shNode);
            if (!orConstraints.isEmpty()) {
                field.put("or", orConstraints);
                List<String> allowedClasses = new ArrayList<>();
                List<String> allowedDatatypes = new ArrayList<>();
                List<String> allowedNodes = new ArrayList<>();
                for (Map<String, Object> constraint : orConstraints) {
                    if (constraint.containsKey("class")) allowedClasses.add((String) constraint.get("class"));
                    if (constraint.containsKey("datatype")) allowedDatatypes.add((String) constraint.get("datatype"));
                    if (constraint.containsKey("node")) allowedNodes.add((String) constraint.get("node"));
                }
                if (!allowedClasses.isEmpty()) field.put("allowedClasses", allowedClasses);
                if (!allowedDatatypes.isEmpty()) field.put("allowedDatatypes", allowedDatatypes);
                if (!allowedNodes.isEmpty()) field.put("allowedNodes", allowedNodes);
            }
        }

        Statement nodeSt = prop.getProperty(shNode);
        if (nodeSt != null && nodeSt.getObject().isResource()) {
            Resource nestedShape = nodeSt.getObject().asResource();
            String nestedShapeId = nestedShape.isURIResource() ? nestedShape.getLocalName() : null;
            if (nestedShapeId != null) {
                field.put("nodeShape", nestedShapeId);
                List<Map<String, Object>> nestedFields = extractPropertyFields(
                    nestedShape, shProperty, shName, shPath, shMinCount, shMaxCount,
                    shOrder, shDescription, shNodeKind, dashEditor, dashViewer,
                    dashSingleLine, shDatatype, shClass, shOr, shNode, shIn,
                    shPattern, shMinLength, shMaxLength
                );
                if (!nestedFields.isEmpty()) field.put("nestedFields", nestedFields);
            }
        }

        return field;
    }

    /**
     * Extract property fields from a shape node (used for both top-level and nested shapes).
     */
    private static List<Map<String, Object>> extractPropertyFields(
            Resource shape, Property shProperty, Property shName, Property shPath,
            Property shMinCount, Property shMaxCount, Property shOrder, Property shDescription,
            Property shNodeKind, Property dashEditor, Property dashViewer, Property dashSingleLine,
            Property shDatatype, Property shClass, Property shOr, Property shNode,
            Property shIn, Property shPattern, Property shMinLength, Property shMaxLength) {

        List<Map<String, Object>> fields = new ArrayList<>();

        shape.listProperties(shProperty).forEach(propStmt -> {
            Resource prop = propStmt.getObject().asResource();
            Map<String, Object> field = extractSingleField(prop,
                shName, shPath, shMinCount, shMaxCount, shOrder, shDescription,
                shNodeKind, dashEditor, dashViewer, dashSingleLine, shDatatype,
                shClass, shOr, shNode, shIn, shPattern, shMinLength, shMaxLength,
                shProperty);
            if (field != null) fields.add(field);
        });

            fields.add(field);
        });

        fields.sort((a, b) -> {
            float oa = a.containsKey("order") ? ((Number) a.get("order")).floatValue() : Float.MAX_VALUE;
            float ob = b.containsKey("order") ? ((Number) b.get("order")).floatValue() : Float.MAX_VALUE;
            return Float.compare(oa, ob);
        });

        return fields;
    }

    /**
     * Extract non-Situation NodeShapes (like Cost_shape) that may be used as nested forms.
     */
    private static void extractNonSituationShapes(
            Map<String, Object> nestedShapes,
            Property typeProp, Resource situationShapeType,
            Property shProperty, Property shName, Property shPath,
            Property shMinCount, Property shMaxCount, Property shOrder, Property shDescription,
            Property shNodeKind, Property dashEditor, Property dashViewer, Property dashSingleLine,
            Property shDatatype, Property shClass, Property shOr, Property shNode,
            Property shIn, Property shPattern, Property shMinLength, Property shMaxLength,
            Property rdfsLabel, Property shDesc) {

        Resource nodeShapeType = ontologyModel.createResource(SH_NS + "NodeShape");

        ontologyModel.listSubjectsWithProperty(typeProp, nodeShapeType).forEach(shape -> {
            // Skip Situation shapes (already handled above)
            if (shape.hasProperty(typeProp, situationShapeType)) return;
            if (!shape.isURIResource()) return;

            String shapeId = shape.getLocalName();
            // Only include shapes that have sh:property declarations
            if (!shape.hasProperty(shProperty)) return;

            Map<String, Object> shapeMap = new LinkedHashMap<>();

            Statement labelSt = shape.getProperty(rdfsLabel);
            if (labelSt != null) shapeMap.put("label", getLiteralString(labelSt));

            Statement descSt = shape.getProperty(shDesc);
            if (descSt != null) shapeMap.put("description", getLiteralString(descSt));

            List<Map<String, Object>> fields = extractPropertyFields(
                shape, shProperty, shName, shPath, shMinCount, shMaxCount,
                shOrder, shDescription, shNodeKind, dashEditor, dashViewer,
                dashSingleLine, shDatatype, shClass, shOr, shNode, shIn,
                shPattern, shMinLength, shMaxLength
            );

            if (!fields.isEmpty()) {
                shapeMap.put("fields", fields);
                nestedShapes.put(shapeId, shapeMap);
            }
        });
    }

    /**
     * Parse an sh:or RDF list into a list of constraint maps.
     * Each element may have sh:class, sh:datatype, or sh:node.
     */
    private static List<Map<String, Object>> parseShOrList(
            RDFNode orNode, Property shClass, Property shDatatype, Property shNode) {

        List<Map<String, Object>> constraints = new ArrayList<>();
        try {
            RDFList rdfList = orNode.as(RDFList.class);
            rdfList.iterator().forEachRemaining(member -> {
                if (!member.isResource()) return;
                Resource memberRes = member.asResource();
                Map<String, Object> constraint = new LinkedHashMap<>();

                Statement classSt = memberRes.getProperty(shClass);
                if (classSt != null && classSt.getObject().isResource()) {
                    constraint.put("class", classSt.getObject().asResource().getLocalName());
                }

                Statement datatypeSt = memberRes.getProperty(shDatatype);
                if (datatypeSt != null && datatypeSt.getObject().isResource()) {
                    constraint.put("datatype", datatypeSt.getObject().asResource().getLocalName());
                }

                Statement nodeSt = memberRes.getProperty(shNode);
                if (nodeSt != null && nodeSt.getObject().isResource()) {
                    constraint.put("node", nodeSt.getObject().asResource().getLocalName());
                }

                if (!constraint.isEmpty()) {
                    constraints.add(constraint);
                }
            });
        } catch (Exception e) {
            // Not a valid list — ignore
        }
        return constraints;
    }

    /**
     * Get string value from a literal statement, stripping any language tag.
     */
    private static String getLiteralString(Statement st) {
        if (st == null) return null;
        RDFNode obj = st.getObject();
        if (obj.isLiteral()) return obj.asLiteral().getString();
        return obj.toString();
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
        Property typeProp  = ontologyModel.createProperty(RDF_NS + "type");

        // ── Bridge: inject lemma nodes from the ontology into the data model ──
        // The SPARQL rules join on the literal value:
        //   ?this :lemma ?lemma .           ← binds ?lemma to "work" (literal)
        //   ?lemmaNode :lemma ?lemma .      ← finds lemma node with same literal
        //   ?lemmaNode :present3sg ?verb .  ← gets the inflected form
        //
        // The lemma node lives in the ontologyModel (loaded from lexical.ttl).
        // RuleUtil executes SPARQL against the dataModel only, so we must copy
        // the relevant lemma node triples into the dataModel for the join to work.

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

        System.out.println("[infer] Lemma bridge: " + lemmas.size() + " lemma(s), "
            + lemmaModel.size() + " triples injected");

        // ── RDFS Entailment ──────────────────────────────────────────────────
        // Compute RDFS closure over the user data + ontology so that:
        //   1. rdfs:subClassOf → subclass type inference
        //      (temp:s5 a :Absorb → also a :Situation, enabling Situation_shape rules)
        //   2. rdfs:range → role player type inference
        //      (temp:s5 :acquirer <NVIDIA> → <NVIDIA> a :Acquirer_Role_Player)
        //   3. rdfs:domain → domain type inference
        //
        // We compute entailments over a union of inputModel + ontologyModel,
        // then extract ONLY the new rdf:type triples about user data resources
        // (not the ontology's own class hierarchy triples).

        Model rdfsEntailments = ModelFactory.createDefaultModel();
        try {
            // Create a union model for RDFS reasoning
            Model unionForRdfs = ModelFactory.createDefaultModel();
            unionForRdfs.add(inputModel);
            unionForRdfs.add(ontologyModel);

            InfModel infModel = ModelFactory.createRDFSModel(unionForRdfs);

            // Extract only rdf:type inferences about resources that appear in the input
            // (subjects or objects of user triples), not ontology-internal triples
            Set<Resource> userResources = new HashSet<>();
            inputModel.listSubjects().forEachRemaining(userResources::add);
            inputModel.listObjects().forEachRemaining(obj -> {
                if (obj.isResource()) userResources.add(obj.asResource());
            });

            for (Resource res : userResources) {
                StmtIterator it = infModel.listStatements(res, typeProp, (RDFNode) null);
                while (it.hasNext()) {
                    Statement stmt = it.next();
                    // Only include types from the ontology namespace (skip RDFS/OWL meta-types)
                    String typeUri = stmt.getObject().isResource()
                        ? stmt.getObject().asResource().getURI() : null;
                    if (typeUri != null && typeUri.startsWith(ONT_NS)) {
                        // Skip if this type was already explicitly asserted in the input
                        if (!inputModel.contains(res, typeProp, stmt.getObject())) {
                            rdfsEntailments.add(
                                rdfsEntailments.createStatement(
                                    rdfsEntailments.createResource(res.getURI()),
                                    rdfsEntailments.createProperty(RDF_NS + "type"),
                                    rdfsEntailments.createResource(typeUri)));
                        }
                    }
                }
            }

            System.out.println("[infer] RDFS entailments: " + rdfsEntailments.size()
                + " new rdf:type triples (subclass + domain + range)");

        } catch (Exception e) {
            System.err.println("[infer] RDFS entailment warning: " + e.getMessage());
            // Non-fatal: continue without entailments
        }

        // ── Assemble data model for SHACL rule execution ─────────────────────
        Model dataModel = ModelFactory.createDefaultModel();
        dataModel.add(inputModel);
        dataModel.add(lemmaModel);
        dataModel.add(rdfsEntailments);  // Include entailed types so sh:targetClass matches
        dataModel.setNsPrefix("",     ONT_NS);
        dataModel.setNsPrefix("temp", "https://falcontologist.github.io/shacl-demo/temp/");
        dataModel.setNsPrefix("rdfs", RDFS_NS);
        dataModel.setNsPrefix("rdf",  RDF_NS);

        // ── Execute SHACL rules ──────────────────────────────────────────────
        Model inferredModel;
        try {
            inferredModel = RuleUtil.executeRules(dataModel, ontologyModel, null, null);
        } catch (Exception e) {
            System.err.println("[infer] Rule execution error: " + e.getMessage());
            e.printStackTrace();
            ctx.status(500).result("Inference error: " + e.getMessage());
            return;
        }

        System.out.println("[infer] SHACL rules inferred: " + inferredModel.size() + " triples");

        // ── Assemble output ──────────────────────────────────────────────────
        // Remove bridge artifacts (lemma, synset, present3sg) but KEEP:
        //   - RDFS entailments (role player types, subclass types)
        //   - SHACL rule inferences (opaque properties)

        dataModel.removeAll(null, dataModel.createProperty(ONT_NS + "lemma"), null);
        dataModel.removeAll(null, dataModel.createProperty(ONT_NS + "synset"), null);
        dataModel.removeAll(null, dataModel.createProperty(ONT_NS + "present3sg"), null);
        dataModel.remove(lemmaModel);
        dataModel.add(inferredModel);
        // rdfsEntailments are already in dataModel and are NOT removed

        StringWriter sw = new StringWriter();
        dataModel.write(sw, "TURTLE");

        long totalInferred = inferredModel.size() + rdfsEntailments.size();

        ctx.json(Map.of(
            "success", true,
            "inferred_data", sw.toString(),
            "stats", Map.of(
                "input_triples",    inputModel.size(),
                "inferred_triples", totalInferred,
                "shacl_triples",    inferredModel.size(),
                "rdfs_triples",     rdfsEntailments.size(),
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
            report.getModel().createProperty(SH_NS + "conforms")
        ).getBoolean();

        StringWriter sw = new StringWriter();
        report.getModel().write(sw, "TURTLE");

        ctx.json(Map.of(
            "conforms",     conforms,
            "report_text",  sw.toString()
        ));
    }

    static Model loadOntology() throws Exception {
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
