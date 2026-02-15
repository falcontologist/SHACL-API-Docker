package com.example;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.topbraid.jenax.util.JenaUtil;
import org.topbraid.shacl.rules.RuleUtil;
import org.topbraid.shacl.validation.ValidationUtil;
import org.topbraid.shacl.vocabulary.SH;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class App {

    private static final String ONT_NS  = "http://example.org/ontology/";

    // Loaded once at startup. Contains classes, properties, shapes, AND rules.
    private static final Model SHAPES_GRAPH = loadShapesGraph();

    private static Model loadShapesGraph() {
        Model m = JenaUtil.createMemoryModel();
        try {
            m.read("roles_shacl.ttl");
            System.out.println("[startup] Loaded roles_shacl.ttl: " + m.size() + " triples.");
        } catch (Exception e) {
            System.err.println("[startup] FAILED to load roles_shacl.ttl: " + e.getMessage());
            e.printStackTrace();
        }
        return m;
    }

    // ─────────────────────────────────────────────────────────────────────────
    public static void main(String[] args) {
        Javalin app = Javalin.create(config ->
            config.bundledPlugins.enableCors(cors -> cors.addRule(it -> it.anyHost()))
        ).start(8000);

        app.get("/",               ctx -> ctx.result("TopBraid SHACL API Online"));
        app.get("/api/stats",      App::getStats);
        app.get("/api/forms",      App::getForms);
        app.get("/api/lookup",     App::lookupVerb);

        // Inference endpoints
        app.post("/api/infer",     App::infer);
        app.get("/api/infer/test", App::inferTest);

        // Validation endpoint
        app.post("/api/validate",  App::validate);
    }

    // ── Core inference helper ─────────────────────────────────────────────────
    private static Model runInference(String turtleBody) {
        System.out.println("\n=== Running Inference ===");
        
        Model dataModel = JenaUtil.createMemoryModel();
        try {
            dataModel.read(new ByteArrayInputStream(turtleBody.getBytes()), null, "TURTLE");
        } catch (Exception e) {
            System.err.println("Failed to parse input Turtle: " + e.getMessage());
            throw e;
        }

        // Merge ontology + user data for inference context
        Model dataWithContext = JenaUtil.createMemoryModel();
        dataWithContext.add(SHAPES_GRAPH);
        dataWithContext.add(dataModel);
        dataWithContext.setNsPrefixes(SHAPES_GRAPH.getNsPrefixMap());
        dataWithContext.setNsPrefixes(dataModel.getNsPrefixMap());

        // Execute SHACL Rules
        Model inferred = RuleUtil.executeRules(dataWithContext, SHAPES_GRAPH, null, null);
        System.out.println("Rules executed. Inferred triples count: " + inferred.size());

        // Add inferences back to the data model for the response
        dataModel.add(inferred);
        dataModel.setNsPrefixes(dataWithContext.getNsPrefixMap());
        
        return dataModel;
    }

    // ── Serialisation helper ──────────────────────────────────────────────────
    private static String serialise(Model m) {
        // Strip internal TopBraid prefixes to keep output clean
        m.removeNsPrefix("dash");
        m.removeNsPrefix("graphql");
        m.removeNsPrefix("swa");
        m.removeNsPrefix("tosh");
        StringWriter sw = new StringWriter();
        RDFDataMgr.write(sw, m, RDFFormat.TURTLE);
        return sw.toString();
    }

    // ── POST /api/infer ───────────────────────────────────────────────────────
    private static void infer(Context ctx) {
        try {
            String body = ctx.body();
            if (body == null || body.isBlank()) {
                ctx.status(400).json(Map.of("error", "Empty request body"));
                return;
            }
            Model expanded = runInference(body);
            ctx.contentType("text/turtle").result(serialise(expanded));
        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", "Inference error", "details", e.getMessage()));
        }
    }

    // ── GET /api/infer/test ───────────────────────────────────────────────────
    private static void inferTest(Context ctx) {
        try {
            Map<String, Object> results = new LinkedHashMap<>();

            // Ensure test2.ttl is in the working directory (COPY in Dockerfile)
            String testData = loadTestFile("test2.ttl");
            
            System.out.println("\n\n========== RUNNING ALL TESTS FROM test2.ttl ==========");
            Model expanded = runInference(testData);
            
            results.put("test_results", Map.of(
                "loaded_file", "test2.ttl",
                "triple_count", expanded.size(),
                "expanded_ttl", serialise(expanded)
            ));
            
            ctx.json(results);
            
        } catch (IOException e) {  
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", "Failed to load test2.ttl: " + e.getMessage()));
        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", "Test inference failed: " + e.getMessage()));
        }
    }

    private static String loadTestFile(String filename) throws IOException {
        return Files.readString(Paths.get(filename));
    }

    // ── POST /api/validate ────────────────────────────────────────────────────
    private static void validate(Context ctx) {
        try {
            Model dataModel = JenaUtil.createMemoryModel();
            try {
                dataModel.read(new ByteArrayInputStream(ctx.bodyAsBytes()), null, "TURTLE");
            } catch (Exception e) {
                ctx.status(400).json(Map.of(
                    "error", "Turtle parsing error", "details", e.getMessage()));
                return;
            }

            boolean conforms   = false;
            String  reportText = "";
            try {
                Resource report = ValidationUtil.validateModel(dataModel, SHAPES_GRAPH, true);
                conforms = report.getProperty(SH.conforms).getBoolean();
                StringWriter sw = new StringWriter();
                RDFDataMgr.write(sw, report.getModel(), RDFFormat.TURTLE);
                reportText = sw.toString();
            } catch (Exception valEx) {
                reportText = "Validation engine error: " + valEx.getMessage();
            }

            ctx.json(Map.of("conforms", conforms, "report_text", reportText));

        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", "Validation error", "details", e.getMessage()));
        }
    }

    // ── GET /api/stats ────────────────────────────────────────────────────────
    private static void getStats(Context ctx) {
        long shapes = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).toList().size();

        Set<String> roles = new HashSet<>();
        SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).forEachRemaining(s ->
            s.listProperties(SH.property).forEachRemaining(p -> {
                if (p.getResource().hasProperty(SH.path))
                    roles.add(p.getResource().getPropertyResourceValue(SH.path).getURI());
            })
        );

        long tripleRules = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.TripleRule).toList().size();
        long sparqlRules = SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.SPARQLRule).toList().size();

        long lemmas = SHAPES_GRAPH.listSubjectsWithProperty(
            RDF.type, SHAPES_GRAPH.createResource(ONT_NS + "Verb")).toList().size();
        long senses = SHAPES_GRAPH.listStatements(
            null, SHAPES_GRAPH.createProperty(ONT_NS + "evokes"), (RDFNode) null).toList().size();

        ctx.json(Map.of(
            "shapes", shapes, "roles", roles.size(),
            "rules",  tripleRules + sparqlRules,
            "lemmas", lemmas,   "synsets", senses
        ));
    }

    // ── GET /api/forms ────────────────────────────────────────────────────────
    private static void getForms(Context ctx) {
        Map<String, Object>              response = new HashMap<>();
        Map<String, Map<String, Object>> forms    = new HashMap<>();

        SHAPES_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).forEachRemaining(shape -> {
            if (!shape.hasProperty(SH.targetClass)) return;
            Resource targetClass = shape.getPropertyResourceValue(SH.targetClass);
            String   name        = targetClass.getLocalName();

            forms.putIfAbsent(name, new HashMap<>(Map.of(
                "target_class", targetClass.getURI(),
                "fields",       new ArrayList<Map<String, Object>>()
            )));

            List<Map<String, Object>> fields        = (List<Map<String, Object>>) forms.get(name).get("fields");
            Set<String>               existingPaths = new HashSet<>();
            for (Map<String, Object> f : fields) existingPaths.add((String) f.get("path"));

            shape.listProperties(SH.property).forEachRemaining(p -> {
                Resource prop = p.getResource();
                if (!prop.hasProperty(SH.path)) return;

                String path = prop.getPropertyResourceValue(SH.path).getURI();
                if (existingPaths.contains(path)) return;

                String   type     = "text";
                Resource datatype = prop.getPropertyResourceValue(SH.datatype);
                if (datatype != null && (datatype.equals(XSD.integer) || datatype.equals(XSD.xint)))
                    type = "number";

                String  label    = prop.hasProperty(SH.name)
                    ? prop.getProperty(SH.name).getString()
                    : path.substring(path.lastIndexOf('/') + 1);
                boolean required = prop.hasProperty(SH.minCount)
                    && prop.getProperty(SH.minCount).getInt() > 0;

                fields.add(Map.of("path", path, "label", label, "type", type, "required", required));
            });
        });

        Property semanticDomainProp = SHAPES_GRAPH.createProperty(ONT_NS + "semantic_domain");
        SHAPES_GRAPH.listSubjectsWithProperty(semanticDomainProp).forEachRemaining(situation -> {
            Resource domain        = situation.getPropertyResourceValue(semanticDomainProp);
            String   situationName = situation.getLocalName();
            String   domainName    = domain.getLocalName();
            if (!forms.containsKey(domainName)) return;

            forms.putIfAbsent(situationName, new HashMap<>(Map.of(
                "target_class", situation.getURI(),
                "fields",       new ArrayList<Map<String, Object>>()
            )));

            List<Map<String, Object>> sitFields    = (List<Map<String, Object>>) forms.get(situationName).get("fields");
            List<Map<String, Object>> domainFields = (List<Map<String, Object>>) forms.get(domainName).get("fields");
            Set<String>               existing     = new HashSet<>();
            for (Map<String, Object> f : sitFields) existing.add((String) f.get("path"));
            for (Map<String, Object> f : domainFields)
                if (!existing.contains((String) f.get("path"))) sitFields.add(f);
        });

        response.put("forms", forms);
        ctx.json(response);
    }

    // ── GET /api/lookup ───────────────────────────────────────────────────────
    private static void lookupVerb(Context ctx) {
        String query = ctx.queryParam("verb");
        if (query == null) { ctx.json(Map.of("found", false)); return; }
        String verb = query.trim().toLowerCase();

        List<Map<String, String>> mappings           = new ArrayList<>();
        Property                  evokesProp         = SHAPES_GRAPH.createProperty(ONT_NS + "evokes");
        Property                  semanticDomainProp = SHAPES_GRAPH.createProperty(ONT_NS + "semantic_domain");

        ResIterator verbs = SHAPES_GRAPH.listSubjectsWithProperty(evokesProp);
        while (verbs.hasNext()) {
            Resource v = verbs.next();
            if (v.hasProperty(RDFS.label)
                    && v.getProperty(RDFS.label).getString().toLowerCase().equals(verb)) {
                StmtIterator evokes = v.listProperties(evokesProp);
                while (evokes.hasNext()) {
                    Resource situation = evokes.next().getResource();
                    Resource domain    = situation.hasProperty(semanticDomainProp)
                        ? situation.getPropertyResourceValue(semanticDomainProp)
                        : situation;
                    mappings.add(Map.of(
                        "situation",       situation.getLocalName(),
                        "fallback_domain", domain.getLocalName()
                    ));
                }
            }
        }
        ctx.json(Map.of("found", !mappings.isEmpty(), "mappings", mappings));
    }
}
