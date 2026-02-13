package com.example;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.topbraid.shacl.rules.RuleUtil;
import org.topbraid.shacl.validation.ValidationUtil;
import org.topbraid.shacl.vocabulary.SH;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class App {
    private static final Model UNIFIED_GRAPH = RDFDataMgr.loadModel("roles_shacl.ttl");
    private static final String ONT_NS = "http://example.org/ontology/";

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

    private static void getForms(Context ctx) {
        Map<String, Object> response = new HashMap<>();
        Map<String, Map<String, Object>> forms = new HashMap<>();

        UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).forEachRemaining(shape -> {
            if (!shape.hasProperty(SH.targetClass)) return;
            Resource targetClass = shape.getPropertyResourceValue(SH.targetClass);
            String name = targetClass.getLocalName();

            // Initialize the Form Entry for this Domain
            forms.putIfAbsent(name, new HashMap<>(Map.of(
                "target_class", targetClass.getURI(),
                "fields", new ArrayList<Map<String, Object>>()
            )));

            Map<String, Object> formData = forms.get(name);
            List<Map<String, Object>> fields = (List<Map<String, Object>>) formData.get("fields");

            // Track existing paths to prevent duplicates when aggregating
            Set<String> existingPaths = new HashSet<>();
            for(Map<String, Object> f : fields) existingPaths.add((String) f.get("path"));

            shape.listProperties(SH.property).forEachRemaining(p -> {
                Resource prop = p.getResource();
                if (!prop.hasProperty(SH.path)) return;
                
                String path = prop.getPropertyResourceValue(SH.path).getURI();
                if (existingPaths.contains(path)) return; // Already have this field

                // Determine Field Type (Text vs Number)
                String type = "text";
                Resource datatype = prop.getPropertyResourceValue(SH.datatype);
                if (datatype != null && (datatype.equals(XSD.integer) || datatype.equals(XSD.xint))) {
                    type = "number";
                }
                
                String label = prop.hasProperty(SH.name) ? prop.getProperty(SH.name).getString() : path.substring(path.lastIndexOf('/')+1);
                boolean required = prop.hasProperty(SH.minCount) && prop.getProperty(SH.minCount).getInt() > 0;

                fields.add(Map.of(
                    "path", path, 
                    "label", label, 
                    "type", type, 
                    "required", required
                ));
            });
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
                    
                    // WORKFLOW LOGIC: Verb -> Situation -> Semantic Domain
                    Resource domain = situation;
                    if (situation.hasProperty(semanticDomainProp)) {
                        domain = situation.getPropertyResourceValue(semanticDomainProp);
                    }
                    
                    String name = domain.getLocalName();
                    mappings.add(Map.of
