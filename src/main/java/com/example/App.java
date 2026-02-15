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

    // All resources use temp: IRIs — no bnodes anywhere.
    // Situation nodes: temp:s<n>
    // Entity nodes:    temp:<EntityName>
    private static final String BASE_PREFIXES =
        "@prefix :     <http://example.org/ontology/> .\n" +
        "@prefix temp: <http://example.org/temp/> .\n"     +
        "@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\n";

    // ── Test inputs ───────────────────────────────────────────────────────────
    // Both use the same verb "acquire" and identical sense gloss.
    // They differ only in which entities fill the roles.
    //
    // Pre-computed expected opaque IRI:
    //   SHA256("acquire|to obtain ownership or possession of")
    //   = 2bd8509722e07bf3...  → first 12 chars: 2bd8509722e0
    //   → :acquires_2bd8509722e0
    //
    // BOTH tests must produce this identical IRI — that is the determinism proof.

    private static final String EXPECTED_OPAQUE_IRI =
        ONT_NS + "acquires_2bd8509722e0";

    // Test 1: first form-meaning encoding — Alphabet acquires Wiz
    private static final String TEST_1 =
        BASE_PREFIXES +
        "temp:s1 a :Acquisition ;\n" +
        "    :root        \"acquire\" ;\n" +
        "    :sense       \"to obtain ownership or possession of\" ;\n" +
        "    :acquirer    temp:Alphabet ;\n" +
        "    :acquisition temp:Wiz .\n\n" +
        "temp:Alphabet a :Entity ; rdfs:label \"Alphabet\" .\n" +
        "temp:Wiz      a :Entity ; rdfs:label \"Wiz\" .\n";

    // Test 2: second encoding — same verb + same gloss, different entities
    // Must mint the IDENTICAL opaque IRI as Test 1.
    private static final String TEST_2 =
        BASE_PREFIXES +
        "temp:s2 a :Acquisition ;\n" +
        "    :root        \"acquire\" ;\n" +
        "    :sense       \"to obtain ownership or possession of\" ;\n" +
        "    :acquirer    temp:Google ;\n" +
        "    :acquisition temp:YouTube .\n\n" +
        "temp:Google   a :Entity ; rdfs:label \"Google\" .\n" +
        "temp:YouTube  a :Entity ; rdfs:label \"YouTube\" .\n";

    // Loaded once at startup. Contains classes, properties, shapes, AND rules.
    private static final Model SHAPES_GRAPH = loadShapesGraph();

    private static Model loadShapesGraph() {
    Model m = JenaUtil.createMemoryModel();
    try {
        m.read("roles_shacl.ttl");
        System.out.println("[startup] Loaded roles_shacl.ttl: " + m.size() + " triples.");
        
        // DEBUG: Print ALL rules with their full details
        System.out.println("\n=== ALL SHACL RULES IN SHAPES GRAPH ===");
        
        // Check for TripleRules
        System.out.println("\n-- TripleRules --");
        ResIterator tripleRules = m.listSubjectsWithProperty(RDF.type, SH.TripleRule);
        while (tripleRules.hasNext()) {
            Resource rule = tripleRules.next();
            System.out.println("TripleRule URI: " + rule.getURI());
            if (rule.hasProperty(SH.construct)) {
                System.out.println("  CONSTRUCT: " + rule.getProperty(SH.construct).getString());
            }
            if (rule.hasProperty(SH.order)) {
                System.out.println("  ORDER: " + rule.getProperty(SH.order).getInt());
            }
        }
        
        // Check for SPARQLRules
        System.out.println("\n-- SPARQLRules --");
        ResIterator sparqlRules = m.listSubjectsWithProperty(RDF.type, SH.SPARQLRule);
        while (sparqlRules.hasNext()) {
            Resource rule = sparqlRules.next();
            System.out.println("SPARQLRule URI: " + rule.getURI());
            if (rule.hasProperty(SH.construct)) {
                System.out.println("  CONSTRUCT: " + rule.getProperty(SH.construct).getString());
            }
            if (rule.hasProperty(SH.order)) {
                System.out.println("  ORDER: " + rule.getProperty(SH.order).getInt());
            }
        }
        
        // Also check for any property that might be the opaque one
        System.out.println("\n-- Checking for acquires_* properties --");
        ResIterator allSubjects = m.listSubjects();
        while (allSubjects.hasNext()) {
            Resource subject = allSubjects.next();
            if (subject.isURIResource() && subject.getURI().contains("acquires_")) {
                System.out.println("Found acquires_* property: " + subject.getURI());
                // Show what kind of resource it is
                if (subject.hasProperty(RDF.type)) {
                    System.out.println("  Type: " + subject.getProperty(RDF.type).getObject());
                }
            }
        }
        
        System.out.println("=== END RULE DEBUG ===\n");
        
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
        System.out.println("\n=== Running Inference ===");
        System.out.println("Input Turtle: \n" + turtleBody);
        
        Model dataModel = JenaUtil.createMemoryModel();
        try {
            dataModel.read(new ByteArrayInputStream(turtleBody.getBytes()), null, "TURTLE");
            System.out.println("Parsed data model size: " + dataModel.size() + " triples");
        } catch (Exception e) {
            System.err.println("Failed to parse input Turtle: " + e.getMessage());
            throw e;
        }

        Model dataWithContext = JenaUtil.createMemoryModel();
        dataWithContext.add(SHAPES_GRAPH);
        dataWithContext.add(dataModel);
        dataWithContext.setNsPrefixes(SHAPES_GRAPH.getNsPrefixMap());
        dataWithContext.setNsPrefixes(dataModel.getNsPrefixMap());

        System.out.println("Data with context size (shapes + input): " + dataWithContext.size() + " triples");

        // Debug: Check for Acquisition instances before rule execution
        System.out.println("\nAcquisition instances before rules:");
        StmtIterator beforeAcq = dataWithContext.listStatements(
            null, 
            RDF.type, 
            dataWithContext.createResource(ONT_NS + "Acquisition")
        );
        while (beforeAcq.hasNext()) {
            Statement stmt = beforeAcq.next();
            System.out.println("  Found: " + stmt.getSubject());
        }

        // Execute rules
        System.out.println("\nExecuting SHACL rules...");
        Model inferred = RuleUtil.executeRules(dataWithContext, SHAPES_GRAPH, null, null);
        
        System.out.println("Rules executed. Inferred triples count: " + inferred.size());
        
        // Debug: Print all inferred triples
        if (inferred.size() > 0) {
            System.out.println("\nInferred triples:");
            inferred.write(System.out, "TURTLE");
        } else {
            System.out.println("WARNING: No triples were inferred!");
        }

        dataModel.add(inferred);
        dataModel.setNsPrefixes(dataWithContext.getNsPrefixMap());
        
        System.out.println("Final model size: " + dataModel.size() + " triples");
        System.out.println("=== Inference Complete ===\n");
        
        return dataModel;
    }

    // ── Serialisation helper ──────────────────────────────────────────────────
    // Strips TopBraid-internal prefixes that leak from the SHAPES_GRAPH merge.
    private static String serialise(Model m) {
        m.removeNsPrefix("dash");
        m.removeNsPrefix("graphql");
        m.removeNsPrefix("swa");
        m.removeNsPrefix("tosh");
        StringWriter sw = new StringWriter();
        RDFDataMgr.write(sw, m, RDFFormat.TURTLE);
        return sw.toString();
    }

    // ── Opaque IRI detection helper ───────────────────────────────────────────
    // Finds the first :acquires_* property that is:
    //   a) declared rdfs:subPropertyOf :acquires
    //   b) actually used as a predicate in at least one triple
    // Returns the URI string, or "(not found)".
    private static String findMintedOpaqueIRI(Model m) {
        System.out.println("\nSearching for opaque IRI...");
        
        // First, list all subproperties of :acquires
        Property acquires = m.createProperty(ONT_NS + "acquires");
        StmtIterator it = m.listStatements(null, RDFS.subPropertyOf, acquires);
        
        while (it.hasNext()) {
            Resource prop = it.next().getSubject();
            if (prop.isURIResource() && prop.getURI().startsWith(ONT_NS + "acquires_")) {
                System.out.println("Found potential opaque property: " + prop.getURI());
                
                // Check if this property is actually used
                boolean isUsed = m.contains(null, m.createProperty(prop.getURI()), (RDFNode) null);
                System.out.println("  Is used in triples: " + isUsed);
                
                if (isUsed) {
                    // Print the triples that use this property
                    StmtIterator used = m.listStatements(null, m.createProperty(prop.getURI()), (RDFNode) null);
                    while (used.hasNext()) {
                        System.out.println("  Usage: " + used.next());
                    }
                    return prop.getURI();
                }
            }
        }
        
        // If not found via subproperty, also check directly for properties with the pattern
        System.out.println("Checking for properties matching pattern acquires_* directly...");
        StmtIterator allProps = m.listStatements(null, null, (RDFNode) null);
        Set<String> foundProps = new HashSet<>();
        while (allProps.hasNext()) {
            Statement stmt = allProps.next();
            String predicate = stmt.getPredicate().getURI();
            if (predicate != null && predicate.startsWith(ONT_NS + "acquires_")) {
                foundProps.add(predicate);
                System.out.println("Found direct usage: " + predicate + " in triple: " + stmt);
            }
        }
        
        if (!foundProps.isEmpty()) {
            return foundProps.iterator().next();
        }
        
        return "(not found)";
    }

    // ── POST /api/infer ───────────────────────────────────────────────────────
    // Accepts:  text/turtle — data graph (temp: IRIs throughout)
    // Returns:  text/turtle — input + all inferred triples
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
    // Runs two test cases with the same verb+gloss pairing on different entities.
    //
    // Test 1: temp:Alphabet acquires temp:Wiz     (first encoding)
    // Test 2: temp:Google   acquires temp:YouTube (second encoding)
    //
    // Both use:  :root "acquire" + :sense "to obtain ownership or possession of"
    // Both must mint: :acquires_2bd8509722e0  (SHA256 determinism proof)
    //
    // Per-test assertions:
    //   simple_rule_fired    — :acquires triple present (sh:order 1)
    //   opaque_rule_fired    — :acquires_<hash> triple present (sh:order 2)
    //   minted_iri           — the actual minted property URI
    //   iri_matches_expected — minted IRI == :acquires_2bd8509722e0
    //
    // Cross-test assertion:
    //   same_iri_both_tests  — BOTH tests produced the exact same IRI
    //   verdict              — PASS / FAIL / PENDING
    private static void inferTest(Context ctx) {
        try {
            Map<String, Object> results = new LinkedHashMap<>();

            // ── Test 1: Alphabet acquires Wiz ────────────────────────────────
            System.out.println("\n\n========== TEST 1: Alphabet acquires Wiz ==========");
            Model expanded1 = runInference(TEST_1);

            Property acquires1 = expanded1.createProperty(ONT_NS + "acquires");
            Resource alphabet  = expanded1.createResource(TEMP_NS + "Alphabet");
            Resource wiz       = expanded1.createResource(TEMP_NS + "Wiz");

            boolean simple1 = expanded1.contains(alphabet, acquires1, wiz);
            System.out.println("Test 1 - Simple rule fired: " + simple1);
            
            String  iri1    = findMintedOpaqueIRI(expanded1);
            System.out.println("Test 1 - Minted IRI: " + iri1);
            
            boolean opaque1 = !iri1.equals("(not found)");
            if (opaque1) {
                opaque1 = expanded1.contains(alphabet, expanded1.createProperty(iri1), wiz);
                System.out.println("Test 1 - Opaque property used with correct entities: " + opaque1);
            }

            results.put("test1_Alphabet_acquires_Wiz", Map.of(
                "simple_rule_fired",    simple1,
                "opaque_rule_fired",    opaque1,
                "minted_iri",           iri1,
                "iri_matches_expected", EXPECTED_OPAQUE_IRI.equals(iri1),
                "expected_iri",         EXPECTED_OPAQUE_IRI,
                "expanded_ttl",         serialise(expanded1)
            ));

            // ── Test 2: Google acquires YouTube ──────────────────────────────
            System.out.println("\n\n========== TEST 2: Google acquires YouTube ==========");
            Model expanded2 = runInference(TEST_2);

            Property acquires2 = expanded2.createProperty(ONT_NS + "acquires");
            Resource google    = expanded2.createResource(TEMP_NS + "Google");
            Resource youtube   = expanded2.createResource(TEMP_NS + "YouTube");

            boolean simple2 = expanded2.contains(google, acquires2, youtube);
            System.out.println("Test 2 - Simple rule fired: " + simple2);
            
            String  iri2    = findMintedOpaqueIRI(expanded2);
            System.out.println("Test 2 - Minted IRI: " + iri2);
            
            boolean opaque2 = !iri2.equals("(not found)");
            if (opaque2) {
                opaque2 = expanded2.contains(google, expanded2.createProperty(iri2), youtube);
                System.out.println("Test 2 - Opaque property used with correct entities: " + opaque2);
            }

            results.put("test2_Google_acquires_YouTube", Map.of(
                "simple_rule_fired",    simple2,
                "opaque_rule_fired",    opaque2,
                "minted_iri",           iri2,
                "iri_matches_expected", EXPECTED_OPAQUE_IRI.equals(iri2),
                "expected_iri",         EXPECTED_OPAQUE_IRI,
                "expanded_ttl",         serialise(expanded2)
            ));

            // ── Cross-test: determinism proof ─────────────────────────────────
            boolean sameIRI = iri1.equals(iri2) && !iri1.equals("(not found)");

            String verdict;
            if (sameIRI) {
                verdict = "PASS — same form-meaning pair correctly maps to same property IRI";
            } else if (opaque1 || opaque2) {
                verdict = "FAIL — opaque rule fired but IRIs differ (hash non-determinism)";
            } else {
                verdict = "PENDING — add :Acquisition_OpaqueIRIRule to roles_shacl.ttl";
            }

            results.put("determinism_proof", Map.of(
                "same_iri_both_tests", sameIRI,
                "test1_iri",           iri1,
                "test2_iri",           iri2,
                "expected_iri",        EXPECTED_OPAQUE_IRI,
                "verdict",             verdict
            ));

            // Print summary
            System.out.println("\n\n========== TEST SUMMARY ==========");
            System.out.println("Test 1 IRI: " + iri1);
            System.out.println("Test 2 IRI: " + iri2);
            System.out.println("Expected IRI: " + EXPECTED_OPAQUE_IRI);
            System.out.println("Same IRI: " + sameIRI);
            System.out.println("Verdict: " + verdict);
            System.out.println("===================================\n");

            ctx.json(results);

        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).json(Map.of(
                "error",   "Test inference failed",
                "details", e.getMessage()
            ));
        }
    }

    // ── POST /api/validate ────────────────────────────────────────────────────
    // Constraint checking only — no inference.
    // Pass expanded TTL from /api/infer if inferred triples affect validation.
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
            "rules",  tripleRules + sparqlRules,
            "lemmas", lemmas,   "senses", senses
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
