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

    private static final String BASE_PREFIXES =
        "@prefix :     <http://example.org/ontology/> .\n" +
        "@prefix temp: <http://example.org/temp/> .\n"     +
        "@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\n";

    // ── Test inputs ───────────────────────────────────────────────────────────

    // Test 1: Simple rule baseline — blank node situation, temp: named entities.
    // Already confirmed passing. Exercises Acquisition_InferenceRule (sh:order 1).
    // Expected: temp:Alphabet :acquires temp:Wiz
    private static final String TEST_1_SIMPLE =
        BASE_PREFIXES +
        "[ a :Acquisition ;\n" +
        "  :acquirer    temp:Alphabet ;\n" +
        "  :acquisition temp:Wiz ] .\n\n" +
        "temp:Alphabet a :Entity ; rdfs:label \"Alphabet\" .\n" +
        "temp:Wiz      a :Entity ; rdfs:label \"Wiz\" .\n";

    // Test 2: SHA256 opaque-IRI rule — temp: named entities.
    // Exercises both rules. Requires :root, :sense, and :present3sg on :acquire verb.
    // Expected:
    //   temp:Google :acquires temp:YouTube .
    //   temp:Google :acquires_<sha256hash> temp:YouTube .
    //   :acquires_<hash> a rdf:Property ; rdfs:subPropertyOf :acquires .
    private static final String TEST_2_SHA256_TEMP =
        BASE_PREFIXES +
        "[ a :Acquisition ;\n" +
        "  :root        \"acquire\" ;\n" +
        "  :sense       \"to obtain ownership or possession of\" ;\n" +
        "  :acquirer    temp:Google ;\n" +
        "  :acquisition temp:YouTube ] .\n\n" +
        "temp:Google  a :Entity ; rdfs:label \"Google\" .\n" +
        "temp:YouTube a :Entity ; rdfs:label \"YouTube\" .\n";

    // Test 3: SHA256 opaque-IRI rule — pure blank nodes as acquirer/acquisition.
    // Tests whether bnodes can carry inferred triples and whether IDs are stable.
    // Expected: _:buyer :acquires _:asset and _:buyer :acquires_<hash> _:asset
    // Watch for: Jena may re-label bnodes in serialisation output.
    private static final String TEST_3_SHA256_BNODES =
        BASE_PREFIXES +
        "[ a :Acquisition ;\n" +
        "  :root        \"acquire\" ;\n" +
        "  :sense       \"to obtain ownership or possession of\" ;\n" +
        "  :acquirer    _:buyer ;\n" +
        "  :acquisition _:asset ] .\n\n" +
        "_:buyer a :Entity ; rdfs:label \"Buyer (bnode)\" .\n" +
        "_:asset a :Entity ; rdfs:label \"Asset (bnode)\" .\n";

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

        // Inference: materialises inferred triples, no constraint checking
        app.post("/api/infer",     App::infer);
        app.get("/api/infer/test", App::inferTest);

        // Validation: constraint checking only, no inference
        app.post("/api/validate",  App::validate);
    }

    // ── Core inference helper ─────────────────────────────────────────────────
    private static Model runInference(String turtleBody) {
        Model dataModel = JenaUtil.createMemoryModel();
        dataModel.read(new ByteArrayInputStream(turtleBody.getBytes()), null, "TURTLE");

        Model dataWithContext = JenaUtil.createMemoryModel();
        dataWithContext.add(SHAPES_GRAPH);
        dataWithContext.add(dataModel);
        dataWithContext.setNsPrefixes(SHAPES_GRAPH.getNsPrefixMap());
        dataWithContext.setNsPrefixes(dataModel.getNsPrefixMap());

        Model inferred = RuleUtil.executeRules(dataWithContext, SHAPES_GRAPH, null, null);

        dataModel.add(inferred);
        dataModel.setNsPrefixes(dataWithContext.getNsPrefixMap());
        return dataModel;
    }

    // ── POST /api/infer ───────────────────────────────────────────────────────
    // Accepts:  text/turtle — data graph from the frontend form
    // Returns:  text/turtle — input + all inferred triples
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
            ctx.status(500).json(Map.of("error", "Inference error", "details", e.getMessage()));
        }
    }

    // ── GET /api/infer/test ───────────────────────────────────────────────────
    // Runs all three test cases and returns a structured comparison.
    //
    // Test 1 — simple rule, temp: entities          (baseline, confirmed passing)
    // Test 2 — SHA256 opaque-IRI rule, temp: entities
    // Test 3 — SHA256 opaque-IRI rule, pure bnodes
    //
    // The SHA256 rule is defined in test.ttl and loaded at startup via
    // roles_shacl.ttl (once promoted). Until then, Test 2 and Test 3 will show
    // rule_fired: false — that's the signal that the rule needs to be added
    // to roles_shacl.ttl.
    private static void inferTest(Context ctx) {
        try {
            Map<String, Object> results = new LinkedHashMap<>();

            // ── Test 1: simple rule, temp: entities ──────────────────────────
            {
                Model expanded = runInference(TEST_1_SIMPLE);

                Property acquiresProp = expanded.createProperty(ONT_NS + "acquires");
                Resource alphabet     = expanded.createResource(TEMP_NS + "Alphabet");
                Resource wiz          = expanded.createResource(TEMP_NS + "Wiz");
                boolean  fired        = expanded.contains(alphabet, acquiresProp, wiz);

                results.put("test1_simple_rule", Map.of(
                    "description",     "Simple :acquires rule, temp: named entities (baseline)",
                    "expected_triple", "temp:Alphabet  :acquires  temp:Wiz",
                    "rule_fired",      fired,
                    "expanded_ttl",    serialise(expanded)
                ));
            }

            // ── Test 2: SHA256 rule, temp: entities ───────────────────────────
            // The opaque IRI is deterministic: SHA256("acquire|to obtain ownership or possession of")
            // We can't compute it in Java here without re-implementing the hash,
            // so we check for the EXISTENCE of any :acquires_* property subPropertyOf :acquires,
            // and for a triple temp:Google <any-acquires-variant> temp:YouTube.
            {
                Model expanded = runInference(TEST_2_SHA256_TEMP);

                Resource google   = expanded.createResource(TEMP_NS + "Google");
                Resource youtube  = expanded.createResource(TEMP_NS + "YouTube");
                Property acquires = expanded.createProperty(ONT_NS + "acquires");

                // Check simple rule fired (order 1)
                boolean simpleFired = expanded.contains(google, acquires, youtube);

                // Check opaque-IRI rule fired (order 2):
                // Look for any property whose URI starts with :acquires_ and
                // which is declared rdfs:subPropertyOf :acquires
                boolean opaqueFired = false;
                String  opaqueIRI   = "(not found)";
                StmtIterator subProps = expanded.listStatements(
                    null,
                    RDFS.subPropertyOf,
                    expanded.createResource(ONT_NS + "acquires")
                );
                while (subProps.hasNext()) {
                    Resource prop = subProps.next().getSubject();
                    if (prop.isURIResource()
                            && prop.getURI().startsWith(ONT_NS + "acquires_")) {
                        // Confirm this property was actually used in a triple
                        if (expanded.contains(google, expanded.createProperty(prop.getURI()), youtube)) {
                            opaqueFired = true;
                            opaqueIRI   = prop.getURI();
                        }
                    }
                }

                results.put("test2_sha256_temp_entities", Map.of(
                    "description",          "SHA256 opaque-IRI rule, temp: named entities",
                    "expected_simple",      "temp:Google  :acquires  temp:YouTube",
                    "expected_opaque",      "temp:Google  :acquires_<sha256>  temp:YouTube",
                    "simple_rule_fired",    simpleFired,
                    "opaque_rule_fired",    opaqueFired,
                    "minted_property_iri",  opaqueIRI,
                    "expanded_ttl",         serialise(expanded)
                ));
            }

            // ── Test 3: SHA256 rule, pure bnodes ──────────────────────────────
            // Key questions:
            //   a) Does the rule fire at all when acquirer/acquisition are bnodes?
            //   b) What IDs does Jena assign the bnodes in the output?
            //   c) Are those IDs stable across re-runs? (Check by running twice.)
            {
                Model expandedRun1 = runInference(TEST_3_SHA256_BNODES);
                Model expandedRun2 = runInference(TEST_3_SHA256_BNODES); // second run

                String ttlRun1 = serialise(expandedRun1);
                String ttlRun2 = serialise(expandedRun2);

                // Check if any :acquires triple exists with bnode subjects
                Property acquires    = expandedRun1.createProperty(ONT_NS + "acquires");
                boolean  simpleFired = expandedRun1.contains(null, acquires, (RDFNode) null);

                // Check for opaque IRI subproperty
                boolean opaqueFired = false;
                String  opaqueIRI   = "(not found)";
                StmtIterator subProps = expandedRun1.listStatements(
                    null,
                    RDFS.subPropertyOf,
                    expandedRun1.createResource(ONT_NS + "acquires")
                );
                while (subProps.hasNext()) {
                    Resource prop = subProps.next().getSubject();
                    if (prop.isURIResource()
                            && prop.getURI().startsWith(ONT_NS + "acquires_")) {
                        if (expandedRun1.contains(null,
                                expandedRun1.createProperty(prop.getURI()), (RDFNode) null)) {
                            opaqueFired = true;
                            opaqueIRI   = prop.getURI();
                        }
                    }
                }

                // Bnode stability: compare the two TTL outputs
                // If bnodes are stable, the serialisations will be identical.
                // If not, the bnode labels (_:b0, etc.) will differ.
                boolean bnodeLabelsStable = ttlRun1.equals(ttlRun2);

                results.put("test3_sha256_bnodes", Map.of(
                    "description",         "SHA256 opaque-IRI rule, pure blank nodes as acquirer/acquisition",
                    "simple_rule_fired",   simpleFired,
                    "opaque_rule_fired",   opaqueFired,
                    "minted_property_iri", opaqueIRI,
                    "bnode_labels_stable", bnodeLabelsStable,
                    "note_on_stability",   "If false: bnodes re-labelled between runs — use temp: IRIs or skolemize",
                    "expanded_ttl_run1",   ttlRun1,
                    "expanded_ttl_run2",   ttlRun2
                ));
            }

            ctx.json(results);

        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).json(Map.of(
                "error",   "Test inference failed",
                "details", e.getMessage()
            ));
        }
    }

    // ── Serialisation helper ──────────────────────────────────────────────────
    // Strips noisy TopBraid-internal prefixes before returning to the client.
    private static String serialise(Model m) {
        // Remove TopBraid-internal prefixes that leak from SHAPES_GRAPH merge
        m.removeNsPrefix("dash");
        m.removeNsPrefix("graphql");
        m.removeNsPrefix("swa");
        m.removeNsPrefix("tosh");
        StringWriter sw = new StringWriter();
        RDFDataMgr.write(sw, m, RDFFormat.TURTLE);
        return sw.toString();
    }

    // ── POST /api/validate ────────────────────────────────────────────────────
    // Constraint checking only — no inference.
    // Pass the expanded TTL from /api/infer if inferred triples affect validation.
    private static void validate(Context ctx) {
        try {
            Model dataModel = JenaUtil.createMemoryModel();
            try {
                dataModel.read(new ByteArrayInputStream(ctx.bodyAsBytes()), null, "TURTLE");
            } catch (Exception e) {
                ctx.status(400).json(Map.of("error", "Turtle parsing error", "details", e.getMessage()));
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
            "rules", tripleRules + sparqlRules,
            "lemmas", lemmas, "senses", senses
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
