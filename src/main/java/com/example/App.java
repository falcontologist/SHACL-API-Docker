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
    // UNIFIED_GRAPH must be a JenaUtil memory model so TopBraid's internal
    // SHACL/DASH system namespace registrations are available at load time.
    private static final Model UNIFIED_GRAPH = loadUnifiedGraph();
    private static final String ONT_NS = "http://example.org/ontology/";

    private static Model loadUnifiedGraph() {
        Model m = JenaUtil.createMemoryModel();
        m.read("roles_shacl.ttl");
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
    }

    private static void validate(Context ctx) {
        try {
            // 1. Load user data into a JenaUtil memory model.
            //    JenaUtil.createMemoryModel() — not ModelFactory.createDefaultModel() —
            //    registers the SHACL/DASH system namespaces that TopBraid needs internally.
            Model dataModel = JenaUtil.createMemoryModel();
            dataModel.read(new ByteArrayInputStream(ctx.bodyAsBytes()), null, "TURTLE");
            dataModel.setNsPrefixes(UNIFIED_GRAPH.getNsPrefixMap());

            // 2. Merge: give RuleUtil a combined view of ontology + user data so the
            //    SPARQL WHERE clauses in sh:SPARQLRule blocks can resolve ontology-level
            //    terms ($this type checks, property declarations, etc.) alongside user triples.
            Model dataWithContext = JenaUtil.createMemoryModel();
            dataWithContext.add(UNIFIED_GRAPH);
            dataWithContext.add(dataModel);
            dataWithContext.setNsPrefixes(UNIFIED_GRAPH.getNsPrefixMap());

            // 3. Execute SHACL Advanced Features rules.
            //    Rules are read from UNIFIED_GRAPH; they execute against dataWithContext.
            //    The shapes model (UNIFIED_GRAPH) already carries the @prefix declarations
            //    that sh:prefixes resolution depends on — this is why the model type matters.
            Model inferred = RuleUtil.executeRules(dataWithContext, UNIFIED_GRAPH, null, null);

            // --- DEBUG: print inferred triples to console ---
            System.out.println("==== INFERRED TRIPLES START ====");
            RDFDataMgr.write(System.out, inferred, RDFFormat.TURTLE);
            System.out.println("==== INFERRED TRIPLES END ====");

            // 4. Add only the net-new inferred triples into the user model.
            dataModel.add(inferred);

            // 5. Validate the enriched user data against the shapes.
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
            ctx.status(500).result("Error: " + e.getMessage());
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

        // Form Inheritance Logic
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
        ctx.json(Map.of("shapes", shapes, "roles", roles.size(), "lemmas", lemmas, "senses", senses));
    }
}
