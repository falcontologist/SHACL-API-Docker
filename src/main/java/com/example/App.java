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
    private static final Model UNIFIED_GRAPH = loadUnifiedGraph();
    private static final String ONT_NS = "http://example.org/ontology/";

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
        
        // NEW: Diagnostic Endpoint
        app.get("/api/test-rules", App::testRules);
    }

    // --- NEW DIAGNOSTIC METHOD ---
    private static void testRules(Context ctx) {
        Map<String, Object> results = new LinkedHashMap<>();

        // TEST 1: ISOLATED SPARQL RULE
        // We create a tiny standalone graph with a hardcoded SPARQL rule to see if the engine works.
        try {
            Model m = JenaUtil.createMemoryModel();
            String ns = "http://test.org/";
            Resource personClass = m.createResource(ns + "Person");
            Resource ruleShape = m.createResource(ns + "PersonShape");
            Property hasName = m.createProperty(ns + "hasName");
            Property hasLabel = RDFS.label;

            // Define Data
            m.add(m.createResource(ns + "Bob"), RDF.type, personClass);
            m.add(m.createResource(ns + "Bob"), hasName, "Robert");

            // Define SPARQL Rule
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
        // We create a tiny standalone graph with a TripleRule.
        try {
            Model m = JenaUtil.createMemoryModel();
            String ns = "http://test.org/";
            Resource buyClass = m.createResource(ns + "Transaction");
            Resource ruleShape = m.createResource(ns + "TransactionShape");
            Property buyer = m.createProperty(ns + "buyer");
            Property bought = m.createProperty(ns + "bought");
            Property owns = m.createProperty(ns + "owns");

            // Define Data: Bob buys Car
            Resource bob = m.createResource(ns + "Bob");
            Resource car = m.createResource(ns + "Car");
            Resource tx = m.createResource(ns + "tx1");
            m.add(tx, RDF.type, buyClass);
            m.add(tx, buyer, bob);
            m.add(tx, bought, car);

            // Define Triple Rule: subject(buyer) -> predicate(owns) -> object(bought)
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
        // Does the loaded file actually contain the rule?
        try {
            String ruleURI = ONT_NS + "DynamicPossession_InferenceRule";
            Resource actualRule = UNIFIED_GRAPH.getResource(ruleURI);
            boolean exists = UNIFIED_GRAPH.contains(actualRule, RDF.type, SH.NodeShape);

            Map<String, Object> liveTest = new HashMap<>();
            liveTest.put("rule_exists_in_graph", exists);

            if (exists) {
                // Try to run it against sample data
                Model sampleData = JenaUtil.createMemoryModel();
                Resource sit = sampleData.createResource("_:testSit");
                Resource acquirer = sampleData.createResource(ONT_NS + "TestCompany");
                Resource acquisition = sampleData.createResource(ONT_NS + "TestProduct");
                
                sampleData.add(sit, RDF.type, UNIFIED_GRAPH.getResource(ONT_NS + "Dynamic_Possession"));
                sampleData.add(sit, UNIFIED_GRAPH.getProperty(ONT_NS + "acquirer"), acquirer);
                sampleData.add(sit, UNIFIED_GRAPH.getProperty(ONT_NS + "acquisition"), acquisition);

                // Run inference using the UNIFIED_GRAPH as the shapes graph
                Model combined = JenaUtil.createMemoryModel().add(UNIFIED_GRAPH).add(sampleData);
                Model inferred = RuleUtil.executeRules(combined, UNIFIED_GRAPH, null, null);

                Property acquiresProp = UNIFIED_GRAPH.getProperty(ONT_NS + "acquires");
                // Check if TestCompany acquires TestProduct
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
    
    // ... [Keep existing validate, getForms, lookupVerb, getStats methods exactly as they were] ...
    
    private static void validate(Context ctx) {
        try {
            // ... (Your existing validate code) ...
            // Just copying the start for context, keep the full method from previous file
            String incomingData = ctx.body();
            // ...
            Model dataModel = JenaUtil.createMemoryModel();
            try {
                dataModel.read(new ByteArrayInputStream(ctx.bodyAsBytes()), null, "TURTLE");
            } catch (Exception e) {
                ctx.status(400).result("Error parsing Turtle");
                return;
            }

            Model dataWithContext = JenaUtil.createMemoryModel();
            dataWithContext.add(UNIFIED_GRAPH);
            dataWithContext.add(dataModel);

            Model inferred = RuleUtil.executeRules(dataWithContext, UNIFIED_GRAPH, null, null);

            dataModel.add(inferred);
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
            ctx.status(500).result(e.getMessage());
        }
    }

    private static void getForms(Context ctx) {
        // ... (Keep existing getForms code) ...
        // Need to ensure UNIFIED_GRAPH is accessible here. 
        // If you need the full code block for this let me know, 
        // otherwise paste your previous getForms logic here.
        Map<String, Object> response = new HashMap<>();
        // ... implementation from your file ...
        ctx.json(response);
    }

    private static void lookupVerb(Context ctx) {
         // ... (Keep existing lookupVerb code) ...
         // ... implementation from your file ...
         ctx.json(Map.of("found", false)); // placeholder if you copy-paste, use your real code
    }

    private static void getStats(Context ctx) {
        // ... (Keep existing getStats code) ...
        // ... implementation from your file ...
        ctx.json(Map.of("status", "ok")); // placeholder
    }
}
