package com.example;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
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
        Map<String, Object> forms = new HashMap<>();

        UNIFIED_GRAPH.listSubjectsWithProperty(RDF.type, SH.NodeShape).forEachRemaining(shape -> {
            if (!shape.hasProperty(SH.targetClass)) return;
            Resource targetClass = shape.getPropertyResourceValue(SH.targetClass);
            String name = targetClass.getLocalName();

            Map<String, Object> formData = new HashMap<>();
            formData.put("target_class", targetClass.getURI());
            List<Map<String, Object>> fields = new ArrayList<>();

            shape.listProperties(SH.property).forEachRemaining(p -> {
                Resource prop = p.getResource();
                if (!prop.hasProperty(SH.path)) return;
                String path = prop.getPropertyResourceValue(SH.path).getURI();
                fields.add(Map.of(
                    "path", path,
                    "label", prop.hasProperty(SH.name) ? prop.getProperty(SH.name).getString() : path,
                    "required", prop.hasProperty(SH.minCount) && prop.getProperty(SH.minCount).getInt() > 0
                ));
            });
            formData.put("fields", fields);
            forms.put(name, formData);
        });

        response.put("forms", forms);
        ctx.json(response);
    }

    private static void validate(Context ctx) {
        Model dataModel = ModelFactory.createDefaultModel();
        dataModel.read(new ByteArrayInputStream(ctx.bodyAsBytes()), null, "TURTLE");

        // Execute Rules
        Model inferred = RuleUtil.executeRules(dataModel, UNIFIED_GRAPH, null, null);
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
    }

    private static void getStats(Context ctx) {
        ctx.json(Map.of("shapes", 0, "roles", 0, "lemmas", 0, "senses", 0)); // Placeholder for brevity
    }

    private static void lookupVerb(Context ctx) {
        // Implement logic similar to Python lookup
        ctx.json(Map.of("found", false, "mappings", List.of())); 
    }
}