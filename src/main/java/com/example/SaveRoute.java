package com.example;

import org.apache.hc.client5.http.auth.StandardAuthScheme;
import org.apache.hc.client5.http.impl.auth.DigestSchemeFactory;
import org.apache.hc.client5.http.impl.auth.BasicSchemeFactory;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.client5.http.auth.AuthSchemeFactory;
import io.javalin.http.Context;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.Lang;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.ContentType;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SaveRoute {

    private static final String SPARQL_UPDATE_URL =
        System.getenv().getOrDefault(
            "VIRTUOSO_SPARQL_UPDATE_URL",
            "http://virtuoso:8890/sparql-auth"
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

    public static void handle(Context ctx) throws Exception {
        try {
            String turtle = ctx.body();
            if (turtle == null || turtle.isBlank()) {
                ctx.status(400).result("Empty Turtle body");
                return;
            }

            Model model = ModelFactory.createDefaultModel();
            try (InputStream is = new ByteArrayInputStream(
                    turtle.getBytes(StandardCharsets.UTF_8))) {
                RDFDataMgr.read(model, is, Lang.TURTLE);
            } catch (Exception e) {
                System.err.println("[save] Turtle parse error: " + e.getMessage());
                ctx.status(400).result("Invalid Turtle: " + e.getMessage());
                return;
            }

            long tripleCount = model.size();
            System.out.println("[save] Parsed " + tripleCount + " triples");

            StringWriter sw = new StringWriter();
            model.write(sw, "N-TRIPLE");
            String ntriples = sw.toString();
            System.out.println("[save] N-Triples sample:\n" + ntriples.substring(0, Math.min(500, ntriples.length())));

            String sparqlUpdate = String.format(
                "INSERT DATA { GRAPH <%s> {\n%s\n} }",
                TOKEN_GRAPH, ntriples
            );

            // Apache HttpClient handles digest auth automatically
            BasicCredentialsProvider creds = new BasicCredentialsProvider();
            creds.setCredentials(
                new AuthScope(null, -1),
                new UsernamePasswordCredentials(VIRTUOSO_USER, VIRTUOSO_PASSWORD.toCharArray())
            );

            try (CloseableHttpClient client = HttpClients.custom()
                    .setDefaultCredentialsProvider(creds)
                    .setDefaultAuthSchemeRegistry(
                        RegistryBuilder.<AuthSchemeFactory>create()
                            .register(StandardAuthScheme.DIGEST, DigestSchemeFactory.INSTANCE)
                            .register(StandardAuthScheme.BASIC, BasicSchemeFactory.INSTANCE)
                            .build())
                    .build()) {

                HttpPost post = new HttpPost(SPARQL_UPDATE_URL);
                post.setEntity(new StringEntity(sparqlUpdate,
                    ContentType.create("application/sparql-update", StandardCharsets.UTF_8)));

                int statusCode = client.execute(post, response -> {
                    System.out.println("[save] Virtuoso: " + response.getCode());
                    return response.getCode();
                });

                if (statusCode >= 200 && statusCode < 300) {
                    System.out.println("[save] Saved " + tripleCount + " triples to " + TOKEN_GRAPH);
                    ctx.status(200).json(Map.of(
                        "status", "saved",
                        "graph", TOKEN_GRAPH,
                        "tripleCount", tripleCount
                    ));
                } else {
                    ctx.status(502).result("Virtuoso returned " + statusCode);
                }
            }
        } catch (Exception e) {
            System.err.println("[save] EXCEPTION: " + e.getClass().getName() + ": " + e.getMessage());
            e.printStackTrace();
            ctx.status(500).result("Save failed: " + e.getMessage());
        }
    }
}
