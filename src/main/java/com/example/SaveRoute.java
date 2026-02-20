package com.example;

import io.javalin.http.Context;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.Lang;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class SaveRoute {

    private static final String SPARQL_UPDATE_URL =
        System.getenv().getOrDefault(
            "VIRTUOSO_SPARQL_UPDATE_URL",
            "http://localhost:8890/sparql-auth"
        );

    private static final String VIRTUOSO_USER =
        System.getenv().getOrDefault("VIRTUOSO_USER", "sparql_writer");

    private static final String VIRTUOSO_PASSWORD =
        System.getenv().getOrDefault("VIRTUOSO_PASSWORD", "writer_secret");

    private static final String TOKEN_GRAPH =
        System.getenv().getOrDefault(
            "VIRTUOSO_INSTANCE_GRAPH",
            "http://shacl-demo.org/token"
        );

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    public static void handle(Context ctx) throws Exception {
        String turtle = ctx.body();
        if (turtle == null || turtle.isBlank()) {
            ctx.status(400).result("Empty Turtle body");
            return;
        }

        // Validate the Turtle parses cleanly before touching Virtuoso
        Model model = ModelFactory.createDefaultModel();
        try (InputStream is = new ByteArrayInputStream(
                turtle.getBytes(StandardCharsets.UTF_8))) {
            RDFDataMgr.read(model, is, Lang.TURTLE);
        } catch (Exception e) {
            ctx.status(400).result("Invalid Turtle: " + e.getMessage());
            return;
        }

        long tripleCount = model.size();
        System.out.println("[save] Parsed " + tripleCount + " triples");

        // Serialise to N-Triples for safe embedding in SPARQL Update body
        StringWriter sw = new StringWriter();
        model.write(sw, "N-TRIPLE");
        String ntriples = sw.toString();

        // Wrap in SPARQL Update INSERT DATA into the token graph
        String sparqlUpdate = String.format(
            "INSERT DATA { GRAPH <%s> {\n%s\n} }",
            TOKEN_GRAPH,
            ntriples
        );

        // Basic-auth POST to Virtuoso /sparql-auth
        String credentials = Base64.getEncoder().encodeToString(
            (VIRTUOSO_USER + ":" + VIRTUOSO_PASSWORD)
                .getBytes(StandardCharsets.UTF_8)
        );

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(SPARQL_UPDATE_URL))
            .header("Authorization", "Basic " + credentials)
            .header("Content-Type", "application/sparql-update")
            .POST(BodyPublishers.ofString(sparqlUpdate, StandardCharsets.UTF_8))
            .build();

        HttpResponse<String> response = HTTP.send(request, BodyHandlers.ofString());

        if (response.statusCode() >= 200 && response.statusCode() < 300) {
            System.out.println("[save] Saved " + tripleCount + " triples to " + TOKEN_GRAPH);
            ctx.status(200).json(Map.of(
                "status", "saved",
                "graph", TOKEN_GRAPH,
                "tripleCount", tripleCount
            ));
        } else {
            System.err.println("[save] Virtuoso returned " + response.statusCode()
                + ": " + response.body());
            ctx.status(502).result(
                "Virtuoso returned " + response.statusCode() + ": " + response.body()
            );
        }
    }
}
