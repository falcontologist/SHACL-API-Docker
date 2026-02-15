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
import java.io.StringWriter;
import java.util.*;

public class App {

    private static final String ONT_NS  = "http://example.org/ontology/";
    private static final String TEMP_NS = "http://example.org/temp/";

    // Prefix block that mirrors what the frontend sends.
    // temp: is NOT in roles_shacl.ttl — it must come from the client body.
    private static final String BASE_PREFIXES =
        "@prefix :     <http://example.org/ontology/> .\n" +
        "@prefix temp: <http://example.org/temp/> .\n"     +
        "@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\n";

    // Hardcoded test input: mirrors exactly what the frontend produces
    // for an Acquisition entry with temp: entities as acquirer / acquisition.
    // The blank node [ a :Acquisition ; ... ] is the focus node for the rule.
    private static final String TEST_INPUT =
        BASE_PREFIXES +
        "[ a :Acquisition ;\n" +
        "  :acquirer    temp:Alphabet ;\n" +
        "  :acquisition temp:Wiz ] .\n\n" +
        "temp:Alphabet a :Entity ; rdfs:label \"Alphabet\" .\n" +
        "temp:Wiz      a :Entity ; rdfs:label \"Wiz\" .\n";

    // Loaded once at startup from the working directory.
    // Contains classes, properties, SHACL shapes, AND inference rules.
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

        // Inference: materialises inferred triples, no constraint checking
        app.post("/api/infer",     App::infer);
        app.get("/api/infer/test", App::inferTest);

        // Validation: constraint checking only, no inference
        app.post("/api/validate",  App::validate);
    }

    // ── Core inference helper ─────────────────────────────────────────────────
    // Parses turtleBody, merges with SHAPES_GRAPH for rule context, runs
    // RuleUtil.executeRules(), and returns a model containing ONLY the user
    // data plus inferred triples — not the full ontology.
    //
    // Why merge with SHAPES_GRAPH?
    //   The SPARQL WHERE clause in Acquisition_InferenceRule only traverses
    //   $this, :acquirer, :acquisition — all in the data graph.
    //   The merge ensures Jena can resolve the sh:prefixes block
    //   (:OntologyPrefixes) and any future rules that look up :Verb nodes.
    private static Model runInference(String turtleBody) {
        Model dataModel = JenaUtil.createMemoryModel();
        dataModel.read(new ByteArrayInputStream(turtleBody.getBytes()), null, "TURTLE");

        // Merged context: ontology + user data
        // SHAPES_GRAPH is never mutated — we add it into a fresh model
        Model dataWithContext = JenaUtil.createMemoryModel();
        dataWithContext.add(SHAPES_GRAPH);
        dataWithContext.add(dataModel);
        dataWithContext.setNsPrefixes(SHAPES_GRAPH.getNsPrefixMap());
        dataWithContext.setNsPrefixes(dataModel.getNsPrefixMap());

        // Execute all sh:rule entries whose sh:targetClass is matched by data.
        // Returns ONLY the newly inferred triples, not the full merged model.
        Model inferred = RuleUtil.executeRules(dataWithContext, SHAPES_GRAPH, null, null);

        // Attach inferred triples to the user data model (not the ontology)
        dataModel.add(inferred);
        dataModel.setNsPrefixes(dataWithContext.getNsPrefixMap());
        return dataModel;
    }

    // ── POST /api/infer ───────────────────────────────────────────────────────
    // Accepts:  Content-Type: text/turtle
    //           Body: data graph (blank nodes, temp: entities, etc.)
    // Returns:  Content-Type: text/turtle
    //           Body: input graph + all inferred triples
    //
    // Does NOT run SHACL constraint checking.
    //
    // Frontend workflow:
    //   1. POST /api/infer  → receive expanded TTL
    //   2. Show expanded TTL in editor / graph visualiser
    //   3. Optionally POST expanded TTL to /api/validate
    private static void infer(Context ctx) {
        try {
            String body = ctx.body();
            if (body == null || body.isBlank()) {
                ctx.status(400).json(Map.of("error", "Empty request body"));
                return;
            }

            Model expanded = runInference(body);

            StringWriter sw = new StringWriter();
            RDFDataMgr.write(sw, expanded, RDFFormat.TURTLE);
            ctx.contentType("text/turtle").result(sw.toString());

        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).json(Map.of(
                "error",   "Inference error",
                "details", e.getMessage()
            ));
        }
    }

    // ── GET /api/infer/test ───────────────────────────────────────────────────
    // Zero-argument endpoint. Runs inference on the hardcoded TEST_INPUT and
    // returns structured JSON for browser inspection.
    //
    // Input (TEST_INPUT):
    //   [ a :Acquisition ; :acquirer temp:Alphabet ; :acquisition temp:Wiz ]
    //
    // Expected inferred triple (from Acquisition_InferenceRule):
    //   temp:Alphabet  :acquires  temp:Wiz
    //
    // Response fields:
    //   rule_fired       boolean — true if the expected triple is present
    //   expected_triple  string  — human-readable label of the target triple
    //   inferred_triples list    — all triples with a temp: subject
    //   test_input       string  — the Turtle that was submitted
    //   expanded_ttl     string  — the full serialised output graph
    private static void inferTest(Context ctx) {
        try {
            Model expanded = runInference(TEST_INPUT);

            // Check specifically for: temp:Alphabet :acquires temp:Wiz
            Property acquiresProp = expanded.createProperty(ONT_NS + "acquires");
            Resource alphabet     = expanded.createResource(TEMP_NS + "Alphabet");
            Resource wiz          = expanded.createResource(TEMP_NS + "Wiz");
            boolean  ruleFired    = expanded.contains(alphabet, acquiresProp, wiz);

            // Collect triples whose subject is a temp: resource
            List<String> inferredTriples = new ArrayList<>();
            expanded.listStatements().forEachRemaining(stmt -> {
                String subjectUri = stmt.getSubject().isURIResource()
                    ? stmt.getSubject().getURI() : null;
                if (subjectUri != null && subjectUri.startsWith(TEMP_NS)) {
                    inferredTriples.add(
                        "<" + subjectUri + ">  " +
                        "<" + stmt.getPredicate().getURI() + ">  " +
                        stmt.getObject().toString()
                    );
                }
            });

            StringWriter sw = new StringWriter();
            RDFDataMgr.write(sw, expanded, RDFFormat.TURTLE);

            ctx.json(Map.of(
                "test_input",       TEST_INPUT,
                "rule_fired",       ruleFired,
                "expected_triple",  "temp:Alphabet  :acquires  temp:Wiz",
                "inferred_triples", inferredTriples,
                "expanded_ttl",     sw.toString()
            ));

        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).json(Map.of(
                "error",   "Test inference failed",
                "details", e.getMessage()
            ));
        }
    }

    // ── POST /api/validate ────────────────────────────────────────────────────
    // Accepts:  Content-Type: text/turtle
    //           Body: data graph to validate (pass expanded TTL from /api/infer
    //                 if inferred triples are required for constraints to pass)
    // Returns:  JSON { conforms: bool, report_text: string }
    //
    // Does NOT run inference. Separation of concerns:
    //   - Validation failures are purely about constraint violations
    //   - The expanded graph from /api/infer can be inspected before validating
    //   - This method is idempotent and side-effect free
    private static void validate(Context ctx) {
        try {
            Model dataModel = JenaUtil.createMemoryModel();
            try {
                dataModel.read(new ByteArrayInputStream(ctx.bodyAsBytes()), null, "TURTLE");
            } catch (Exception e) {
                ctx.status(400).json(Map.of(
                    "error",   "Turtle parsing error",
                    "details", e.getMessage()
                ));
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
                System.err.println("[validate] Engine error: " + valEx.getMessage());
                reportText = "Validation engine error: " + valEx.getMessage();
            }

            ctx.json(Map.of(
                "conforms",    conforms,
                "report_text", reportText
            ));

        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).json(Map.of(
                "error",   "Validation error",
                "details", e.getMessage()
            ));
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
            "shapes", shapes,
            "roles",  roles.size(),
            "rules",  tripleRules + sparqlRules,
            "lemmas", lemmas,
            "senses", senses
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
                if (datatype != null
                        && (datatype.equals(XSD.integer) || datatype.equals(XSD.xint)))
                    type = "number";

                String  label    = prop.hasProperty(SH.name)
                    ? prop.getProperty(SH.name).getString()
                    : path.substring(path.lastIndexOf('/') + 1);
                boolean required = prop.hasProperty(SH.minCount)
                    && prop.getProperty(SH.minCount).getInt() > 0;

                fields.add(Map.of("path", path, "label", label, "type", type, "required", required));
            });
        });

        // Situations inherit fields from their semantic domain shape
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
