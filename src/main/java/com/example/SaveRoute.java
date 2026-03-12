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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SaveRoute {

    private static final String SPARQL_UPDATE_URL =
        System.getenv().getOrDefault("VIRTUOSO_SPARQL_UPDATE_URL", "http://virtuoso:8890/sparql-auth");

    private static final String VIRTUOSO_USER =
        System.getenv().getOrDefault("VIRTUOSO_USER", "sparql_writer");

    private static final String VIRTUOSO_PASSWORD =
        System.getenv().getOrDefault("VIRTUOSO_PASSWORD", "writer_secret");

    private static final String TOKEN_GRAPH =
        System.getenv().getOrDefault("VIRTUOSO_INSTANCE_GRAPH", "http://shacl-demo.org/token");
        
    // Define the graph where your roles (e.g., :acquirer) are defined so Virtuoso knows where to update them
    private static final String CONCEPTUAL_GRAPH = 
        System.getenv().getOrDefault("VIRTUOSO_CONCEPTUAL_GRAPH", "http://shacl-demo.org/conceptual");

    private static final String ONTOLOGY_NS = "https://falcontologist.github.io/shacl-demo/ontology/";

    public static void handle(Context ctx) throws Exception {
        try {
            String turtle = ctx.body();
            if (turtle == null || turtle.isBlank()) {
                ctx.status(400).result("Empty Turtle body");
                return;
            }

            Model model = ModelFactory.createDefaultModel();
            try (InputStream is = new ByteArrayInputStream(turtle.getBytes(StandardCharsets.UTF_8))) {
                RDFDataMgr.read(model, is, Lang.TURTLE);
            } catch (Exception e) {
                System.err.println("[save] Turtle parse error: " + e.getMessage());
                ctx.status(400).result("Invalid Turtle: " + e.getMessage());
                return;
            }

            long tripleCount = model.size();
            System.out.println("[save] Parsed " + tripleCount + " triples");

            // --- STEP 1: EXTRACT ROLE-FILLER PAIRS FOR LEARNING ---
            List<String> learningQueries = new ArrayList<>();
            StmtIterator iter = model.listStatements();
            while (iter.hasNext()) {
                Statement stmt = iter.nextStatement();
                Property predicate = stmt.getPredicate();
                RDFNode object = stmt.getObject();

                // Filter: Only grab properties in your ontology namespace that point to URIs (entities)
                // Ignore structural predicates like rdf:type or string literals like :textSource
                if (predicate.getURI().startsWith(ONTOLOGY_NS) && object.isURIResource()) {
                    String localName = predicate.getLocalName();
                    // Optional: Add a blacklist of predicates you don't want to learn on
                    if (!localName.equals("textSource") && !localName.equals("lemma")) {
                        String roleUri = predicate.getURI();
                        String fillerUri = object.asResource().getURI();
                        learningQueries.add(buildLearningSparql(roleUri, fillerUri));
                    }
                }
            }

            // --- STEP 2: SAVE THE INSTANCE DATA ---
            StringWriter sw = new StringWriter();
            model.write(sw, "N-TRIPLE");
            String ntriples = sw.toString();

            String instanceInsert = String.format("INSERT DATA { GRAPH <%s> {\n%s\n} }", TOKEN_GRAPH, ntriples);

            BasicCredentialsProvider creds = new BasicCredentialsProvider();
            creds.setCredentials(new AuthScope(null, -1),
                new UsernamePasswordCredentials(VIRTUOSO_USER, VIRTUOSO_PASSWORD.toCharArray()));

            try (CloseableHttpClient client = HttpClients.custom()
                    .setDefaultCredentialsProvider(creds)
                    .setDefaultAuthSchemeRegistry(RegistryBuilder.<AuthSchemeFactory>create()
                        .register(StandardAuthScheme.DIGEST, DigestSchemeFactory.INSTANCE)
                        .register(StandardAuthScheme.BASIC, BasicSchemeFactory.INSTANCE).build())
                    .build()) {

                // Execute Main Instance Save
                HttpPost post = new HttpPost(SPARQL_UPDATE_URL);
                post.setEntity(new StringEntity(instanceInsert, ContentType.create("application/sparql-update", StandardCharsets.UTF_8)));
                
                int statusCode = client.execute(post, response -> response.getCode());

                if (statusCode >= 200 && statusCode < 300) {
                    System.out.println("[save] Saved " + tripleCount + " triples to " + TOKEN_GRAPH);
                    
                    // --- STEP 3: EXECUTE LEARNING UPDATES ---
                    int learningSuccesses = 0;
                    for (String learningQuery : learningQueries) {
                        HttpPost learnPost = new HttpPost(SPARQL_UPDATE_URL);
                        learnPost.setEntity(new StringEntity(learningQuery, ContentType.create("application/sparql-update", StandardCharsets.UTF_8)));
                        int learnStatus = client.execute(learnPost, response -> response.getCode());
                        if (learnStatus >= 200 && learnStatus < 300) {
                            learningSuccesses++;
                        } else {
                            System.err.println("[save] Failed to update role weights. Status: " + learnStatus);
                        }
                    }
                    System.out.println("[save] Processed " + learningSuccesses + " role weight updates.");

                    ctx.status(200).json(Map.of(
                        "status", "saved",
                        "graph", TOKEN_GRAPH,
                        "tripleCount", tripleCount,
                        "rolesUpdated", learningSuccesses
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

    /**
     * Generates the SPARQL query that instructs Virtuoso to update a role's centroid 
     * based on the actual scores of the filler entity.
     */
    private static String buildLearningSparql(String roleUri, String fillerUri) {
        // We use COALESCE in the WHERE clause so that if a role has 0 observations (Cold Start), 
        // it defaults to 0 rather than failing the query.
        return String.format("""
            PREFIX : <%1$s>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

            WITH <%2$s>
            DELETE {
                <%3$s> :observationCount ?oldCount ;
                       :physicalScore ?oldPhys ; :boundedScore ?oldBound ; :locativeScore ?oldLoc ;
                       :animateScore ?oldAnim ; :sentientScore ?oldSent ; :volitionalScore ?oldVol ;
                       :institutionalScore ?oldInst ; :collectiveScore ?oldColl ; :telicScore ?oldTelic ;
                       :symbolicScore ?oldSym ; :scalarScore ?oldScal ; :temporalScore ?oldTemp .
            }
            INSERT {
                <%3$s> :observationCount ?newCount ;
                       :physicalScore ?newPhys ; :boundedScore ?newBound ; :locativeScore ?newLoc ;
                       :animateScore ?newAnim ; :sentientScore ?newSent ; :volitionalScore ?newVol ;
                       :institutionalScore ?newInst ; :collectiveScore ?newColl ; :telicScore ?newTelic ;
                       :symbolicScore ?newSym ; :scalarScore ?newScal ; :temporalScore ?newTemp .
            }
            WHERE {
                # 1. Fetch the actual scores of the entity acting as the filler
                # (Assuming entity data is in the default graph or accessible globally)
                <%4$s> :physicalScore ?fPhys ; :boundedScore ?fBound ; :locativeScore ?fLoc ;
                       :animateScore ?fAnim ; :sentientScore ?fSent ; :volitionalScore ?fVol ;
                       :institutionalScore ?fInst ; :collectiveScore ?fColl ; :telicScore ?fTelic ;
                       :symbolicScore ?fSym ; :scalarScore ?fScal ; :temporalScore ?fTemp .

                # 2. Fetch the current running averages for the role
                OPTIONAL { <%3$s> :observationCount ?rawCount . }
                OPTIONAL { <%3$s> :physicalScore ?rawPhys . }
                OPTIONAL { <%3$s> :boundedScore ?rawBound . }
                OPTIONAL { <%3$s> :locativeScore ?rawLoc . }
                OPTIONAL { <%3$s> :animateScore ?rawAnim . }
                OPTIONAL { <%3$s> :sentientScore ?rawSent . }
                OPTIONAL { <%3$s> :volitionalScore ?rawVol . }
                OPTIONAL { <%3$s> :institutionalScore ?rawInst . }
                OPTIONAL { <%3$s> :collectiveScore ?rawColl . }
                OPTIONAL { <%3$s> :telicScore ?rawTelic . }
                OPTIONAL { <%3$s> :symbolicScore ?rawSym . }
                OPTIONAL { <%3$s> :scalarScore ?rawScal . }
                OPTIONAL { <%3$s> :temporalScore ?rawTemp . }

                # 3. Handle the "Cold Start" (if values don't exist yet, treat as 0)
                BIND(COALESCE(?rawCount, 0) AS ?N)
                BIND(COALESCE(?rawPhys, 0.0) AS ?cPhys) BIND(COALESCE(?rawBound, 0.0) AS ?cBound)
                BIND(COALESCE(?rawLoc, 0.0) AS ?cLoc)   BIND(COALESCE(?rawAnim, 0.0) AS ?cAnim)
                BIND(COALESCE(?rawSent, 0.0) AS ?cSent) BIND(COALESCE(?rawVol, 0.0) AS ?cVol)
                BIND(COALESCE(?rawInst, 0.0) AS ?cInst) BIND(COALESCE(?rawColl, 0.0) AS ?cColl)
                BIND(COALESCE(?rawTelic, 0.0) AS ?cTelic) BIND(COALESCE(?rawSym, 0.0) AS ?cSym)
                BIND(COALESCE(?rawScal, 0.0) AS ?cScal) BIND(COALESCE(?rawTemp, 0.0) AS ?cTemp)

                # 4. Proportional Math: ((Current Average * Count) + New Target Score) / (Count + 1)
                BIND(?N + 1 AS ?newCount)
                BIND(((?cPhys * ?N) + xsd:float(?fPhys)) / ?newCount AS ?newPhys)
                BIND(((?cBound * ?N) + xsd:float(?fBound)) / ?newCount AS ?newBound)
                BIND(((?cLoc * ?N) + xsd:float(?fLoc)) / ?newCount AS ?newLoc)
                BIND(((?cAnim * ?N) + xsd:float(?fAnim)) / ?newCount AS ?newAnim)
                BIND(((?cSent * ?N) + xsd:float(?fSent)) / ?newCount AS ?newSent)
                BIND(((?cVol * ?N) + xsd:float(?fVol)) / ?newCount AS ?newVol)
                BIND(((?cInst * ?N) + xsd:float(?fInst)) / ?newCount AS ?newInst)
                BIND(((?cColl * ?N) + xsd:float(?fColl)) / ?newCount AS ?newColl)
                BIND(((?cTelic * ?N) + xsd:float(?fTelic)) / ?newCount AS ?newTelic)
                BIND(((?cSym * ?N) + xsd:float(?fSym)) / ?newCount AS ?newSym)
                BIND(((?cScal * ?N) + xsd:float(?fScal)) / ?newCount AS ?newScal)
                BIND(((?cTemp * ?N) + xsd:float(?fTemp)) / ?newCount AS ?newTemp)
            }
            """, ONTOLOGY_NS, CONCEPTUAL_GRAPH, roleUri, fillerUri);
    }
}