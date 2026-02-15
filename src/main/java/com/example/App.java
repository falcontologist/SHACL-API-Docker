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

    private static void validate(Context ctx) {
        try {
            // 1. Parse Incoming Turtle
            Model dataModel = JenaUtil.createMemoryModel();
            try {
                dataModel.read(new ByteArrayInputStream(ctx.bodyAsBytes()), null, "TURTLE");
            } catch (Exception e) {
                ctx.status(400).json(Map.of("error", "Turtle parsing error", "details", e.getMessage()));
                return;
            }

            // 2. Merge ontology + user data for inference context
            Model dataWithContext = JenaUtil.createMemoryModel();
            dataWithContext.add(UNIFIED_GRAPH);
            dataWithContext.add(dataModel);
            
            // CRITICAL: Copy prefixes to ensure SPARQL rules can resolve them
            dataWithContext.setNsPrefixes(UNIFIED_GRAPH.getNsPrefixMap());
            dataWithContext.setNsPrefixes(dataModel.getNsPrefixMap());

            // 3. Execute SHACL Rules (Inference)
            Model inferred = RuleUtil.executeRules(dataWithContext, UNIFIED_GRAPH, null, null);

            // 4. Add inferences to data model
            dataModel.add(inferred);
            dataModel.setNsPrefixes(inferred.getNsPrefixMap());

            // 5. Serialize the expanded data (Data + Inference) *before* validation
            // This ensures we have the result even if validation crashes.
            StringWriter swData = new StringWriter();
            RDFDataMgr.write(swData, dataModel, RDFFormat.TURTLE);

            // 6. Validate
            boolean conforms = false;
            String reportText = "";
            
            try {
                Resource report = ValidationUtil.validateModel(dataModel, UNIFIED_GRAPH, true);
                conforms = report.getProperty(SH.conforms).getBoolean();
                StringWriter swReport = new StringWriter();
                RDFDataMgr.write(swReport, report.getModel(), RDFFormat.TURTLE);
                reportText = swReport.toString();
            } catch (Exception valEx) {
                // If validation crashes (e.g. unsupported nodeKind), catch it but return the data!
                System.err.println("Validation Engine Error: " + valEx.getMessage());
                reportText = "Validation Engine Error: " + valEx.getMessage();
                // We deliberately do not fail the request; we still want to return expanded_data
            }

            // 7. Return Response
            ctx.json(Map.of(
                "conforms", conforms,
                "report_text", reportText,
                "expanded_data", swData.toString()
            ));

        } catch (Exception e) {
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", "Processing error", "details", e.getMessage()));
        }
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

        // Count Rules (TripleRules + SPARQLRules)
        long tripleRules = UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, SH.TripleRule).toList().size();
        long sparqlRules = UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, SH.SPARQLRule).toList().size();
        long rules = tripleRules + sparqlRules;

        long lemmas = UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, UNIFIED_GRAPH.createResource(ONT_NS + "Verb")).toList().size();
        long senses = UNIFIED_GRAPH.listStatements(null, UNIFIED_GRAPH.createProperty(ONT_NS + "evokes"), (RDFNode) null).toList().size();
        
        ctx.json(Map.of(
            "shapes", shapes, 
            "roles", roles.size(), 
            "rules", rules,
            "lemmas", lemmas, 
            "senses", senses
        ));
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

    private static void testRules(Context ctx) {
        // kept from previous step for debugging
        ctx.json(Map.of("status", "available")); 
    }
}
