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
    private static final String ONT_NS = "http://example.org/ontology/";
    // Load graph once at startup
    private static final Model UNIFIED_GRAPH = loadUnifiedGraph();

    private static Model loadUnifiedGraph() {
        Model m = JenaUtil.createMemoryModel();
        try {
            m.read("roles_shacl.ttl");
            System.out.println("Loaded roles_shacl.ttl: " + m.size() + " triples.");
        } catch (Exception e) {
            System.err.println("FAILED to load roles_shacl.ttl: " + e.getMessage());
            e.printStackTrace();
        }
        return m;
    }

    public static void main(String[] args) {
        Javalin app = Javalin.create(config -> {
            config.bundledPlugins.enableCors(cors -> cors.addRule(it -> it.anyHost()));
        }).start(8000);

        app.get("/", ctx -> ctx.result("TopBraid SHACL API Online"));
        app.get("/api/stats", App::getStats);
        app.get("/api/forms", App::getForms);
        app.get("/api/lookup", App::lookupVerb);
        app.post("/api/validate", App::validate);
        
        // Diagnostic Endpoint
        app.get("/api/test-rules", App::testRules);
    }

    // --- DIAGNOSTIC METHOD ---
    private static void testRules(Context ctx) {
        Map<String, Object> results = new LinkedHashMap<>();

        // TEST 1: ISOLATED SPARQL RULE
        try {
            Model m = JenaUtil.createMemoryModel();
            String ns = "http://test.org/";
            Resource personClass = m.createResource(ns + "Person");
            Resource ruleShape = m.createResource(ns + "PersonShape");
            Property hasName = m.createProperty(ns + "hasName");
            Property hasLabel = RDFS.label;

            m.add(m.createResource(ns + "Bob"), RDF.type, personClass);
            m.add(m.createResource(ns + "Bob"), hasName, "Robert");

            Resource sparqlRule = m.createResource().addProperty(RDF.type, SH.SPARQLRule);
            sparqlRule.addProperty(SH.construct, 
                "PREFIX ex: <http://test.org/> " +
                "CONSTRUCT { $this <http://www.w3.org/2000/01/rdf-schema#label> ?name . } " +
                "WHERE { $this ex:hasName ?name . }"
            );
            
            ruleShape.addProperty(RDF.type, SH.NodeShape);
            ruleShape.addProperty(SH.targetClass, personClass);
            ruleShape.addProperty(SH.rule, sparqlRule);

            Model inferred = RuleUtil.executeRules(m, m, null, null);
            boolean passed = inferred.contains(m.createResource(ns + "Bob"), hasLabel, "Robert");
            
            results.put("test_1_sparql_isolation", Map.of(
                "status", passed ? "PASS" : "FAIL",
                "inferred_triples", inferred.size(),
                "details", passed ? "SPARQL rule successfully copied name to label" : "No triples inferred"
            ));
        } catch (Exception e) {
            results.put("test_1_sparql_isolation", Map.of("status", "ERROR", "message", e.getMessage()));
        }

        // TEST 2: ISOLATED TRIPLE RULE
        try {
            Model m = JenaUtil.createMemoryModel();
            String ns = "http://test.org/";
            Resource buyClass = m.createResource(ns + "Transaction");
            Resource ruleShape = m.createResource(ns + "TransactionShape");
            Property buyer = m.createProperty(ns + "buyer");
            Property bought = m.createProperty(ns + "bought");
            Property owns = m.createProperty(ns + "owns");

            Resource bob = m.createResource(ns + "Bob");
            Resource car = m.createResource(ns + "Car");
            Resource tx = m.createResource(ns + "tx1");
            m.add(tx, RDF.type, buyClass);
            m.add(tx, buyer, bob);
            m.add(tx, bought, car);

            Resource tripleRule = m.createResource().addProperty(RDF.type, SH.TripleRule);
            tripleRule.addProperty(SH.subject, m.createResource().addProperty(SH.path, buyer));
            tripleRule.addProperty(SH.predicate, owns);
            tripleRule.addProperty(SH.object, m.createResource().addProperty(SH.path, bought));

            ruleShape.addProperty(RDF.type, SH.NodeShape);
            ruleShape.addProperty(SH.targetClass, buyClass);
            ruleShape.addProperty(SH.rule, tripleRule);

            Model inferred = RuleUtil.executeRules(m, m, null, null);
            boolean passed = inferred.contains(bob, owns, car);

            results.put("test_2_triple_rule_isolation", Map.of(
                "status", passed ? "PASS" : "FAIL",
                "inferred_triples", inferred.size(),
                "details", passed ? "Triple rule successfully inferred ownership" : "No triples inferred"
            ));
        } catch (Exception e) {
            results.put("test_2_triple_rule_isolation", Map.of("status", "ERROR", "message", e.getMessage()));
        }

        // TEST 3: LIVE ONTOLOGY CHECK
        try {
            String ruleURI = ONT_NS + "DynamicPossession_InferenceRule";
            Resource actualRule = UNIFIED_GRAPH.getResource(ruleURI);
            boolean exists = UNIFIED_GRAPH.contains(actualRule, RDF.type, SH.NodeShape);

            Map<String, Object> liveTest = new HashMap<>();
            liveTest.put("rule_exists_in_graph", exists);

            if (exists) {
                Model sampleData = JenaUtil.createMemoryModel();
                Resource sit = sampleData.createResource("_:testSit");
                Resource acquirer = sampleData.createResource(ONT_NS + "TestCompany");
                Resource acquisition = sampleData.createResource(ONT_NS + "TestProduct");
                
                sampleData.add(sit, RDF.type, UNIFIED_GRAPH.getResource(ONT_NS + "Dynamic_Possession"));
                sampleData.add(sit, UNIFIED_GRAPH.getProperty(ONT_NS + "acquirer"), acquirer);
                sampleData.add(sit, UNIFIED_GRAPH.getProperty(ONT_NS + "acquisition"), acquisition);

                Model combined = JenaUtil.createMemoryModel().add(UNIFIED_GRAPH).add(sampleData);
                Model inferred = RuleUtil.executeRules(combined, UNIFIED_GRAPH, null, null);

                Property acquiresProp = UNIFIED_GRAPH.getProperty(ONT_NS + "acquires");
                boolean inferenceWorked = inferred.contains(acquirer, acquiresProp, acquisition);

                liveTest.put("inference_simulation", inferenceWorked ? "SUCCESS" : "FAILURE");
                
                StringWriter debugSw = new StringWriter();
                RDFDataMgr.write(debugSw, inferred, RDFFormat.TURTLE);
                liveTest.put("raw_inferred_turtle", debugSw.toString());
            }

            results.put("test_3_live_ontology", liveTest);

        } catch (Exception e) {
            results.put("test_3_live_ontology", Map.of("status", "ERROR", "message", e.getMessage()));
        }

        ctx.json(results);
    }

    // --- CORE API METHODS ---

    private static void validate(Context ctx) {
        try {
            String incomingData = ctx.body();
            Model dataModel = JenaUtil.createMemoryModel();
            
            try {
                dataModel.read(new ByteArrayInputStream(ctx.bodyAsBytes()), null, "TURTLE");
            } catch (Exception e) {
                ctx.status(400).json(Map.of("error", "Turtle parsing error", "details", e.getMessage()));
                return;
            }

            // Merge ontology + user data for inference context
            Model dataWithContext = JenaUtil.createMemoryModel();
            dataWithContext.add(UNIFIED_GRAPH);
            dataWithContext.add(dataModel);

            // Execute Rules
            Model inferred = RuleUtil.executeRules(dataWithContext, UNIFIED_GRAPH, null, null);

            // Add inferences to data model
            dataModel.add(inferred);

            // Validate
            Resource report = ValidationUtil.validateModel(dataModel, UNIFIED_GRAPH, true);

            StringWriter swReport = new StringWriter();
            RDFDataMgr.write(swReport, report.getModel(), RDFFormat.TURTLE);

            StringWriter swData = new StringWriter();
            RDFDataMgr.write(swData, dataModel, RDFFormat.TURTLE);

            ctx.json(Map.of(
                "conforms", report.getProperty(SH.conforms).getBoolean(),
                "report_text", swReport.toString(),
                "expanded_data", swData.toString()
            ));
        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", "Validation error", "details", e.getMessage()));
        }
    }

    private static void getForms(Context ctx) {
        Map<String, Object> response = new HashMap<>();
        Map<String, Map<String, Object>> forms = new HashMap<>();

        UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).forEachRemaining(shape -> {
            if (!shape.hasProperty(SH.targetClass)) return;
            Resource targetClass = shape.getPropertyResourceValue(SH.targetClass);
            String name = targetClass.getLocalName();

            forms.putIfAbsent(name, new HashMap<>(Map.of(
                "target_class", targetClass.getURI(),
                "fields", new ArrayList<Map<String, Object>>()
            )));

            List<Map<String, Object>> fields = (List<Map<String, Object>>) forms.get(name).get("fields");
            Set<String> existingPaths = new HashSet<>();
            for (Map<String, Object> f : fields) existingPaths.add((String) f.get("path"));

            shape.listProperties(SH.property).forEachRemaining(p -> {
                Resource prop = p.getResource();
                if (!prop.hasProperty(SH.path)) return;

                String path = prop.getPropertyResourceValue(SH.path).getURI();
                if (existingPaths.contains(path)) return;

                String type = "text";
                Resource datatype = prop.getPropertyResourceValue(SH.datatype);
                if (datatype != null && (datatype.equals(XSD.integer) || datatype.equals(XSD.xint))) type = "number";

                String label = prop.hasProperty(SH.name) ? prop.getProperty(SH.name).getString() : path.substring(path.lastIndexOf('/') + 1);
                boolean required = prop.hasProperty(SH.minCount) && prop.getProperty(SH.minCount).getInt() > 0;

                fields.add(Map.of("path", path, "label", label, "type", type, "required", required));
            });
        });

        // Inheritance Logic
        Property semanticDomainProp = UNIFIED_GRAPH.createProperty(ONT_NS + "semantic_domain");
        UNIFIED_GRAPH.listSubjectsWithProperty(semanticDomainProp).forEachRemaining(situation -> {
            Resource domain = situation.getPropertyResourceValue(semanticDomainProp);
            String situationName = situation.getLocalName();
            String domainName = domain.getLocalName();

            if (forms.containsKey(domainName)) {
                forms.putIfAbsent(situationName, new HashMap<>(Map.of(
                    "target_class", situation.getURI(),
                    "fields", new ArrayList<Map<String, Object>>()
                )));
                List<Map<String, Object>> sitFields = (List<Map<String, Object>>) forms.get(situationName).get("fields");
                List<Map<String, Object>> domainFields = (List<Map<String, Object>>) forms.get(domainName).get("fields");

                Set<String> existing = new HashSet<>();
                for (Map<String, Object> f : sitFields) existing.add((String) f.get("path"));

                for (Map<String, Object> f : domainFields) {
                    if (!existing.contains((String) f.get("path"))) sitFields.add(f);
                }
            }
        });

        response.put("forms", forms);
        ctx.json(response);
    }

    private static void lookupVerb(Context ctx) {
        String query = ctx.queryParam("verb");
        if (query == null) { ctx.json(Map.of("found", false)); return; }
        String verb = query.trim().toLowerCase();

        List<Map<String, String>> mappings = new ArrayList<>();
        Property labelProp = RDFS.label;
        Property evokesProp = UNIFIED_GRAPH.createProperty(ONT_NS + "evokes");
        Property semanticDomainProp = UNIFIED_GRAPH.createProperty(ONT_NS + "semantic_domain");

        ResIterator verbs = UNIFIED_GRAPH.listSubjectsWithProperty(evokesProp);
        while (verbs.hasNext()) {
            Resource v = verbs.next();
            if (v.hasProperty(labelProp) && v.getProperty(labelProp).getString().toLowerCase().equals(verb)) {
                StmtIterator evokes = v.listProperties(evokesProp);
                while (evokes.hasNext()) {
                    Resource situation = evokes.next().getResource();
                    Resource domain = situation;
                    if (situation.hasProperty(semanticDomainProp)) {
                        domain = situation.getPropertyResourceValue(semanticDomainProp);
                    }
                    mappings.add(Map.of(
                        "situation", situation.getLocalName(),
                        "fallback_domain", domain.getLocalName()
                    ));
                }
            }
        }
        ctx.json(Map.of("found", !mappings.isEmpty(), "mappings", mappings));
    }

    private static void getStats(Context ctx) {
        long shapes = UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).toList().size();
        Set<String> roles = new HashSet<>();
        UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).forEachRemaining(s ->
            s.listProperties(SH.property).forEachRemaining(p -> {
                if (p.getResource().hasProperty(SH.path))
                    roles.add(p.getResource().getPropertyResourceValue(SH.path).getURI());
            })
        );
        long lemmas = UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, UNIFIED_GRAPH.createResource(ONT_NS + "Verb")).toList().size();
        long senses = UNIFIED_GRAPH.listStatements(null, UNIFIED_GRAPH.createProperty(ONT_NS + "evokes"), (RDFNode) null).toList().size();
        
        ctx.json(Map.of("shapes", shapes, "roles", roles.size(), "lemmas", lemmas, "senses", senses, "status", "ok"));
    }
}
