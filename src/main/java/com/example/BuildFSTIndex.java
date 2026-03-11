package com.example;

import com.google.gson.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.io.BufferedReader;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone FST index builder.
 * 
 * Run locally (Mac/Linux with plenty of RAM) to build the Lucene suggest + sense
 * indices, then tar.gz them for upload to GCS.
 *
 * Usage:
 *   cd SHACL-API-Docker
 *   mvn compile
 *   java -cp target/shacl-service-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *        com.example.BuildFSTIndex \
 *        https://fkg-6htt.onrender.com/sparql \
 *        http://shacl-demo.org/type \
 *        ./fst-index
 *
 * This creates ./fst-index/ with subdirectories for each FST + the sense index.
 * Then:
 *   tar -czf fst-index.tar.gz -C fst-index .
 *   gsutil cp fst-index.tar.gz gs://fkg/fst-index.tar.gz
 */
public class BuildFSTIndex {

    private static final String ONT_NS = "https://falcontologist.github.io/shacl-demo/ontology/";
    private static final int PAGE_SIZE = 2000;

    private static final List<String> CATEGORIES = List.of(
        "Person_Entity", "Organization_Entity", "Geopolitical_Entity", "Product_Entity",
        "Unit_Entity", "Occupation_Entity", "Creative_Work_Entity", "Quantity_Dimension_Entity",
        "Location_Entity", "Food_Entity", "Language_Entity", "Organism_Entity",
        "Equity_Entity", "Index_Entity", "Corporate_Bond_Entity", "Government_Bond_Entity"
    );
    private static final Map<String, String> ENTRY_CLASSES = Map.ofEntries(
        Map.entry("Person_Entity", "Person_Entry"),
        Map.entry("Organization_Entity", "Organization_Entry"),
        Map.entry("Geopolitical_Entity", "Geopolitical_Entry"),
        Map.entry("Product_Entity", "Product_Entry"),
        Map.entry("Unit_Entity", "Unit_Entry"),
        Map.entry("Occupation_Entity", "Occupation_Entry"),
        Map.entry("Creative_Work_Entity", "Creative_Work_Entry"),
        Map.entry("Quantity_Dimension_Entity", "Quantity_Dimension_Entry"),
        Map.entry("Location_Entity", "Location_Entry"),
        Map.entry("Food_Entity", "Food_Entry"),
        Map.entry("Language_Entity", "Language_Entry"),
        Map.entry("Organism_Entity", "Organism_Entry"),
        Map.entry("Equity_Entity", "Equity_Entry"),
        Map.entry("Index_Entity", "Index_Entry"),
        Map.entry("Corporate_Bond_Entity", "Corporate_Bond_Entry"),
        Map.entry("Government_Bond_Entity", "Government_Bond_Entry")
    );
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final Gson GSON = new Gson();

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: BuildFSTIndex <sparql-endpoint> <graph-iri> <output-dir>");
            System.err.println("Example: BuildFSTIndex https://fkg-6htt.onrender.com/sparql http://shacl-demo.org/type ./fst-index");
            System.exit(1);
        }

        String sparqlEndpoint = args[0];
        String graphIRI = args[1];
        Path outputDir = Path.of(args[2]);
        Path tsvDir = args.length >= 4 ? Path.of(args[3]) : null;

        System.out.println("=== FST Index Builder ===");
        System.out.println("Endpoint: " + sparqlEndpoint);
        System.out.println("Graph:    " + graphIRI);
        System.out.println("Output:   " + outputDir.toAbsolutePath());
        System.out.println();

        // Resume mode: keep existing FST directories, only rebuild missing ones
        Files.createDirectories(outputDir);

        long startTime = System.currentTimeMillis();

        // Sense index (shared across all categories)
        Path senseDir = outputDir.resolve("sense-index");
        Files.createDirectories(senseDir);
        IndexWriterConfig senseConfig = new IndexWriterConfig(new StandardAnalyzer());
        senseConfig.setRAMBufferSizeMB(256);
        IndexWriter senseWriter = new IndexWriter(FSDirectory.open(senseDir), senseConfig);

        // Stats file
        Map<String, Object> stats = new LinkedHashMap<>();
        Map<String, Long> entityCounts = new LinkedHashMap<>();
        Map<String, Long> entryCounts = new LinkedHashMap<>();

        for (String category : CATEGORIES) {
            String entryClass = ENTRY_CLASSES.get(category);
            System.out.println("── " + category + " (" + entryClass + ") ──");

            // Skip if FST directory already has data (resume mode)
            Path fstCheck = outputDir.resolve("fst-" + category);
            if (Files.exists(fstCheck) && Files.list(fstCheck).findAny().isPresent()) {
                System.out.println("  Already built, skipping (delete fst-" + category + "/ to rebuild)");
                continue;
            }

            // FST directory for this category
            Path fstDir = outputDir.resolve("fst-" + category);
            Files.createDirectories(fstDir);

            List<SuggestEntry> suggestEntries = new ArrayList<>();
            Set<String> seenEntityIRIs = new HashSet<>();
            AtomicLong entryCount = new AtomicLong(0);
            AtomicLong entityCount = new AtomicLong(0);

            // Check for TSV dump file first (faster + no SPARQL issues)
            String catLower = category.replace("_Entity", "").toLowerCase();
            Path tsvFile = tsvDir != null ? tsvDir.resolve(catLower + "_dump.tsv") : null;

            if (tsvFile != null && Files.exists(tsvFile)) {
                System.out.println("  Reading from TSV: " + tsvFile);
                try (BufferedReader br = Files.newBufferedReader(tsvFile)) {
                    String header = br.readLine(); // skip header
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t", -1);
                        if (parts.length < 4) continue;

                        String entryIRI = parts[0];
                        String label = parts[1];
                        String entityIRI = parts[2];
                        String entityLabel = parts[3];
                        String gloss = parts.length > 4 ? parts[4] : "";
                        String identifier = parts.length > 5 && !parts[5].isEmpty() ? parts[5] : null;

                        if (label.isEmpty() || entityIRI.isEmpty()) continue;

                        entryCount.incrementAndGet();

                        String shortGloss = !gloss.isEmpty() && gloss.length() > 120
                            ? gloss.substring(0, 120) + "..." : gloss;

                        suggestEntries.add(new SuggestEntry(
                            label,
                            !entityLabel.isEmpty() ? entityLabel : label,
                            entityIRI,
                            shortGloss
                        ));

                        if (seenEntityIRIs.add(entityIRI)) {
                            entityCount.incrementAndGet();
                        }

                        String senseId = entityIRI;
                        int pos = Math.max(entityIRI.lastIndexOf('#'), entityIRI.lastIndexOf('/'));
                        if (pos >= 0) senseId = entityIRI.substring(pos + 1);

                        Document doc = new Document();
                        doc.add(new StringField("entityIRI", entityIRI, Field.Store.YES));
                        doc.add(new StringField("senseIRI", entityIRI, Field.Store.YES));
                        doc.add(new StringField("senseId", senseId, Field.Store.YES));
                        doc.add(new StoredField("gloss", gloss));
                        doc.add(new StoredField("label", !entityLabel.isEmpty() ? entityLabel : label));
                        doc.add(new StringField("category", category, Field.Store.YES));
                        if (identifier != null) {
                            doc.add(new StoredField("identifier", identifier));
                        }
                        senseWriter.addDocument(doc);

                        if (entryCount.get() % 500000 == 0) {
                            System.out.printf("  %,d entries, %,d unique entities%n",
                                entryCount.get(), entityCount.get());
                        }
                    }
                }
                System.out.printf("  %,d entries, %,d unique entities%n",
                    entryCount.get(), entityCount.get());
            } else {
                // SPARQL fallback with keyset pagination
                boolean hasMore = true;
                int retryCount = 0;
                String lastEntryIRI = null;

                while (hasMore) {
                    String filterClause = lastEntryIRI != null
                        ? "FILTER(?entry > <" + lastEntryIRI + ">)"
                        : "";
                    String sparql = String.format("""
                        PREFIX : <%s>
                        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                        SELECT ?entry ?label ?entityIRI ?entityLabel ?gloss ?identifier WHERE {
                          GRAPH <%s> {
                            ?entry a :%s ;
                                   rdfs:label ?label ;
                                   :sense ?entityIRI .
                            ?entityIRI rdfs:label ?entityLabel .
                            OPTIONAL { ?entityIRI :gloss ?gloss . }
                            OPTIONAL { ?entityIRI :identifier ?identifier . }
                            %s
                          }
                        }
                        ORDER BY ?entry
                        LIMIT %d
                        """, ONT_NS, graphIRI, entryClass, filterClause, PAGE_SIZE);

                    JsonObject json = executeSparqlQuery(sparqlEndpoint, sparql);
                    if (json == null) {
                        retryCount++;
                        System.err.println("  SPARQL query failed (retry " + retryCount + "/3)");
                        if (retryCount <= 3) {
                            try { Thread.sleep(retryCount * 5000L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                            continue;
                        }
                        System.err.println("  Giving up after 3 retries");
                        break;
                    }
                    retryCount = 0;

                    JsonArray bindings = json.getAsJsonObject("results").getAsJsonArray("bindings");
                    int pageSize = bindings.size();

                    for (JsonElement el : bindings) {
                        JsonObject row = el.getAsJsonObject();
                        String label = getVal(row, "label");
                        String entityIRI = getVal(row, "entityIRI");
                        String entityLabel = getVal(row, "entityLabel");
                        String gloss = getVal(row, "gloss");
                        String identifier = getVal(row, "identifier");

                        if (label == null || entityIRI == null) continue;

                        String entryIRI = getVal(row, "entry");
                        if (entryIRI != null) lastEntryIRI = entryIRI;

                        entryCount.incrementAndGet();

                        String shortGloss = gloss != null && gloss.length() > 120
                            ? gloss.substring(0, 120) + "..." : (gloss != null ? gloss : "");

                        suggestEntries.add(new SuggestEntry(
                            label,
                            entityLabel != null ? entityLabel : label,
                            entityIRI,
                            shortGloss
                        ));

                        if (seenEntityIRIs.add(entityIRI)) {
                            entityCount.incrementAndGet();
                        }

                        String senseId = entityIRI;
                        int pos = Math.max(entityIRI.lastIndexOf('#'), entityIRI.lastIndexOf('/'));
                        if (pos >= 0) senseId = entityIRI.substring(pos + 1);

                        Document doc = new Document();
                        doc.add(new StringField("entityIRI", entityIRI, Field.Store.YES));
                        doc.add(new StringField("senseIRI", entityIRI, Field.Store.YES));
                        doc.add(new StringField("senseId", senseId, Field.Store.YES));
                        doc.add(new StoredField("gloss", gloss != null ? gloss : ""));
                        doc.add(new StoredField("label", entityLabel != null ? entityLabel : label));
                        doc.add(new StringField("category", category, Field.Store.YES));
                        if (identifier != null) {
                            doc.add(new StoredField("identifier", identifier));
                        }
                        senseWriter.addDocument(doc);
                    }

                    hasMore = pageSize == PAGE_SIZE;

                    if (entryCount.get() % 50000 == 0 || !hasMore) {
                        System.out.printf("  %,d entries, %,d unique entities%n",
                            entryCount.get(), entityCount.get());
                    }
                }
            }

            // Build FST to disk
            AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(
                FSDirectory.open(fstDir),
                new StandardAnalyzer()
            );

            if (!suggestEntries.isEmpty()) {
                suggester.build(new SuggestEntryIterator(suggestEntries));
                System.out.printf("  FST built: %,d entities, %,d label variants%n",
                    entityCount.get(), suggestEntries.size());
            } else {
                System.out.println("  (empty)");
            }

            suggester.close();
            entityCounts.put(category, entityCount.get());
            entryCounts.put(entryClass, entryCount.get());

            // Free memory between categories
            suggestEntries.clear();
            seenEntityIRIs.clear();
            System.gc();
        }

        // Finalize sense index
        senseWriter.commit();
        senseWriter.close();

        long elapsed = System.currentTimeMillis() - startTime;

        // Write stats.json
        stats.put("entityCounts", entityCounts);
        stats.put("entryCounts", entryCounts);
        stats.put("buildTimeMs", elapsed);
        stats.put("categories", CATEGORIES);
        stats.put("graphIRI", graphIRI);

        Path statsFile = outputDir.resolve("stats.json");
        Files.writeString(statsFile, GSON.toJson(stats));

        System.out.println();
        System.out.println("=== Build Complete ===");
        System.out.printf("Time: %,d ms%n", elapsed);
        System.out.println("Entity counts: " + entityCounts);
        System.out.println("Entry counts:  " + entryCounts);
        System.out.println("Output: " + outputDir.toAbsolutePath());
        System.out.println();
        System.out.println("Next steps:");
        System.out.println("  tar -czf fst-index.tar.gz -C " + outputDir + " .");
        System.out.println("  gsutil cp fst-index.tar.gz gs://fkg/fst-index.tar.gz");
    }

    // ── SPARQL client ────────────────────────────────────────────────────────

    private static JsonObject executeSparqlQuery(String endpoint, String sparql) throws Exception {
        String encoded = URLEncoder.encode(sparql, StandardCharsets.UTF_8);
        String url = endpoint + "?query=" + encoded + "&format=json";

        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Accept", "application/sparql-results+json")
            .GET()
            .timeout(java.time.Duration.ofSeconds(1200))
            .build();

        HttpResponse<String> resp = HTTP.send(req, BodyHandlers.ofString());

        if (resp.statusCode() != 200) {
            System.err.println("SPARQL error " + resp.statusCode() + ": " +
                resp.body().substring(0, Math.min(200, resp.body().length())));
            return null;
        }

        return GSON.fromJson(resp.body(), JsonObject.class);
    }

    private static String getVal(JsonObject row, String varName) {
        JsonObject binding = row.getAsJsonObject(varName);
        if (binding == null) return null;
        JsonElement val = binding.get("value");
        return val != null ? val.getAsString() : null;
    }

    // ── Data structures ──────────────────────────────────────────────────────

    private record SuggestEntry(String searchText, String primaryLabel, String iri, String gloss) {}

    private static class SuggestEntryIterator implements org.apache.lucene.search.suggest.InputIterator {
        private final Iterator<SuggestEntry> iter;
        private SuggestEntry current;

        SuggestEntryIterator(List<SuggestEntry> entries) {
            this.iter = entries.iterator();
        }

        @Override
        public BytesRef next() {
            if (iter.hasNext()) {
                current = iter.next();
                return new BytesRef(current.searchText);
            }
            return null;
        }

        @Override
        public BytesRef payload() {
            String g = current.gloss != null ? current.gloss : "";
            return new BytesRef(current.primaryLabel + "\t" + current.iri + "\t" + g);
        }

        @Override public boolean hasPayloads() { return true; }
        @Override public long weight() { return 1; }
        @Override public boolean hasContexts() { return false; }
        @Override public Set<BytesRef> contexts() { return null; }
    }
}
