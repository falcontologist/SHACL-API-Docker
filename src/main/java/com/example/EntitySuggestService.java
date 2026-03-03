package com.example;

import com.google.gson.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hybrid entity autocomplete: Lucene FST for sub-10ms suggest, Virtuoso SPARQL for data.
 *
 * Architecture:
 *   - At startup, paginated SPARQL queries stream entries from Virtuoso
 *   - Per-category AnalyzingInfixSuggester (FST) built in-memory (~100-300MB for 2M+ entities)
 *   - Auxiliary Lucene index for sense lookups by entity IRI
 *   - Runtime autocomplete hits FST (sub-10ms), not Virtuoso
 *   - Sense resolution queries Virtuoso (single-IRI lookup, fast)
 *
 * Memory comparison:
 *   - Jena in-memory model for 3GB TTL: ~4-6GB RAM → OOM
 *   - Lucene FST for same data: ~100-300MB RAM → fits easily
 */
public class EntitySuggestService {

    private static final String ONT_NS = "https://falcontologist.github.io/shacl-demo/ontology/";

    public static final List<String> CATEGORIES = List.of(
        "Person_Entity", "Organization_Entity", "Geopolitical_Entity", "Product_Entity"
    );

    public static final Map<String, String> ENTRY_CLASSES = Map.of(
        "Person_Entity", "Person_Entry",
        "Organization_Entity", "Organization_Entry",
        "Geopolitical_Entity", "Geopolitical_Entry",
        "Product_Entity", "Product_Entry"
    );

    // Per-category FST suggester for sub-10ms autocomplete
    private final Map<String, AnalyzingInfixSuggester> suggesters = new ConcurrentHashMap<>();

    // Shared Lucene index for sense lookups (entityIRI → senses)
    private DirectoryReader senseReader;
    private IndexSearcher senseSearcher;

    // Virtuoso SPARQL endpoint for data loading + sense fallback
    private final String sparqlEndpoint;
    private final String graphIRI;
    private final HttpClient http;
    private final Gson gson;

    // Stats
    private final Map<String, Long> entityCounts = new ConcurrentHashMap<>();
    private final Map<String, Long> entryCounts = new ConcurrentHashMap<>();
    private long totalSenses = 0;
    private volatile boolean ready = false;
    private long buildTimeMs = 0;

    // Page size for SPARQL streaming
    private static final int PAGE_SIZE = 5000;

    public EntitySuggestService(String sparqlEndpoint, String graphIRI) {
        this.sparqlEndpoint = sparqlEndpoint;
        this.graphIRI = graphIRI;
        this.http = HttpClient.newHttpClient();
        this.gson = new Gson();
    }

    /**
     * Build FST indices by streaming entries from Virtuoso via paginated SPARQL.
     * Call once at startup. Runs in ~30-120s for millions of entities.
     */
    public void buildIndexFromVirtuoso() throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("[entity-suggest] Building FST indices from Virtuoso SPARQL...");
        System.out.println("[entity-suggest] Endpoint: " + sparqlEndpoint);
        System.out.println("[entity-suggest] Graph: " + graphIRI);

        // Sense index (shared across categories)
        ByteBuffersDirectory senseDir = new ByteBuffersDirectory();
        IndexWriterConfig senseConfig = new IndexWriterConfig(new StandardAnalyzer());
        senseConfig.setRAMBufferSizeMB(64);
        IndexWriter senseWriter = new IndexWriter(senseDir, senseConfig);

        for (String category : CATEGORIES) {
            String entryClass = ENTRY_CLASSES.get(category);
            System.out.println("[entity-suggest]   Streaming category: " + category
                + " (entries: " + entryClass + ")");

            List<SuggestEntry> suggestEntries = new ArrayList<>();
            Set<String> seenEntityIRIs = new HashSet<>();
            AtomicLong entryCount = new AtomicLong(0);
            AtomicLong entityCount = new AtomicLong(0);

            // Paginated SPARQL streaming
            int offset = 0;
            boolean hasMore = true;

            while (hasMore) {
                String sparql = String.format("""
                    PREFIX : <%s>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    SELECT ?entry ?label ?entityIRI ?entityLabel WHERE {
                      GRAPH <%s> {
                        ?entry a :%s ;
                               rdfs:label ?label ;
                               :sense ?entityIRI .
                        ?entityIRI rdfs:label ?entityLabel .
                      }
                    }
                    OFFSET %d LIMIT %d
                    """, ONT_NS, graphIRI, entryClass, offset, PAGE_SIZE);

                JsonObject json = executeSparqlQuery(sparql);
                if (json == null) {
                    System.err.println("[entity-suggest]     SPARQL query failed at offset " + offset);
                    break;
                }

                JsonArray bindings = json.getAsJsonObject("results").getAsJsonArray("bindings");
                int pageSize = bindings.size();

                for (JsonElement el : bindings) {
                    JsonObject row = el.getAsJsonObject();

                    String label = getVal(row, "label");
                    String entityIRI = getVal(row, "entityIRI");
                    String entityLabel = getVal(row, "entityLabel");

                    if (label == null || entityIRI == null) continue;

                    entryCount.incrementAndGet();

                    // Add to FST suggester — search by entry label, payload = entity info
                    suggestEntries.add(new SuggestEntry(
                        label,
                        entityLabel != null ? entityLabel : label,
                        entityIRI,
                        category,
                        null  // gloss fetched on-demand via sense lookup
                    ));

                    // Index senses (one doc per entry→entity link for sense lookups)
                    if (seenEntityIRIs.add(entityIRI)) {
                        entityCount.incrementAndGet();
                    }

                    // Always index the sense mapping
                    String senseId = entityIRI;
                    int pos = Math.max(entityIRI.lastIndexOf('#'), entityIRI.lastIndexOf('/'));
                    if (pos >= 0) senseId = entityIRI.substring(pos + 1);

                    Document doc = new Document();
                    doc.add(new StringField("entityIRI", entityIRI, Field.Store.YES));
                    doc.add(new StringField("senseIRI", entityIRI, Field.Store.YES));
                    doc.add(new StringField("senseId", senseId, Field.Store.YES));
                    doc.add(new StoredField("gloss", ""));
                    doc.add(new StoredField("label", entityLabel != null ? entityLabel : label));
                    doc.add(new StringField("category", category, Field.Store.YES));

                    try {
                        senseWriter.addDocument(doc);
                    } catch (IOException e) {
                        System.err.println("[entity-suggest] Sense index error: " + e.getMessage());
                    }
                }

                offset += PAGE_SIZE;
                hasMore = pageSize == PAGE_SIZE;

                if (offset % 50000 == 0 || !hasMore) {
                    System.out.println("[entity-suggest]     " + category + ": "
                        + entryCount.get() + " entries, "
                        + entityCount.get() + " unique entities streamed...");
                }
            }

            // Build the FST suggester for this category
            AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(
                new ByteBuffersDirectory(),
                new StandardAnalyzer()
            );

            if (!suggestEntries.isEmpty()) {
                suggester.build(new SuggestEntryIterator(suggestEntries));
                System.out.println("[entity-suggest]     " + category + " FST built: "
                    + entityCount.get() + " entities, "
                    + suggestEntries.size() + " label variants");
            } else {
                System.out.println("[entity-suggest]     " + category + ": 0 entries (empty)");
            }

            suggesters.put(category, suggester);
            entityCounts.put(category, entityCount.get());
            entryCounts.put(ENTRY_CLASSES.get(category), entryCount.get());
        }

        // Finalize sense index
        senseWriter.commit();
        senseWriter.close();
        senseReader = DirectoryReader.open(senseDir);
        senseSearcher = new IndexSearcher(senseReader);
        totalSenses = senseReader.numDocs();

        buildTimeMs = System.currentTimeMillis() - startTime;
        System.out.println("[entity-suggest] Index build complete in " + buildTimeMs + "ms");
        System.out.println("[entity-suggest]   Total entities: "
            + entityCounts.values().stream().mapToLong(Long::longValue).sum());
        System.out.println("[entity-suggest]   Total senses indexed: " + totalSenses);

        ready = true;
    }

    /**
     * Sub-10ms autocomplete via Lucene FST.
     */
    public List<Map<String, String>> suggest(String category, String query, int limit) throws Exception {
        if (!ready) return List.of();

        AnalyzingInfixSuggester suggester = suggesters.get(category);
        if (suggester == null) return List.of();

        List<Lookup.LookupResult> results = suggester.lookup(query, false, limit);

        // Deduplicate by IRI (multiple label variants may match)
        LinkedHashMap<String, Map<String, String>> deduped = new LinkedHashMap<>();
        for (Lookup.LookupResult result : results) {
            String payload = result.payload != null ? result.payload.utf8ToString() : "";
            // payload format: "primaryLabel\tiri\tgloss"
            String[] parts = payload.split("\t", 3);
            if (parts.length < 2) continue;

            String primaryLabel = parts[0];
            String iri = parts[1];
            String gloss = parts.length > 2 ? parts[2] : "";
            String matchedText = result.key.toString();

            if (!deduped.containsKey(iri)) {
                Map<String, String> entry = new LinkedHashMap<>();
                entry.put("label", primaryLabel);
                entry.put("iri", iri);
                entry.put("category", category);
                if (!gloss.isEmpty()) entry.put("gloss", gloss);

                // Extract local name for display
                String localName = iri;
                int pos = Math.max(iri.lastIndexOf('#'), iri.lastIndexOf('/'));
                if (pos >= 0) localName = iri.substring(pos + 1);
                entry.put("localName", localName);

                // If matched on a different label variant, show it
                if (!matchedText.equals(primaryLabel)) {
                    entry.put("matchedLabel", matchedText);
                }
                deduped.put(iri, entry);
            }
        }

        return new ArrayList<>(deduped.values());
    }

    /**
     * Sense lookup: first try local Lucene index, fall back to Virtuoso SPARQL.
     */
    public List<Map<String, String>> getSenses(String entityIRI) throws Exception {
        // Try local Lucene index first (fast)
        if (ready && senseSearcher != null) {
            List<Map<String, String>> local = getSensesFromLucene(entityIRI);
            if (!local.isEmpty()) return local;
        }

        // Fallback to Virtuoso SPARQL
        return getSensesFromVirtuoso(entityIRI);
    }

    private List<Map<String, String>> getSensesFromLucene(String entityIRI) throws Exception {
        // Find all entries that share this entity → get their other senses
        TermQuery query = new TermQuery(new Term("entityIRI", entityIRI));
        TopDocs topDocs = senseSearcher.search(query, 50);

        List<Map<String, String>> senses = new ArrayList<>();
        for (ScoreDoc sd : topDocs.scoreDocs) {
            Document doc = senseSearcher.storedFields().document(sd.doc);
            Map<String, String> sense = new LinkedHashMap<>();
            sense.put("senseId", doc.get("senseId"));
            sense.put("senseIRI", doc.get("senseIRI"));
            sense.put("gloss", doc.get("gloss"));
            sense.put("label", doc.get("label"));
            String identifier = doc.get("identifier");
            if (identifier != null) sense.put("identifier", identifier);
            senses.add(sense);
        }

        return senses;
    }

    private List<Map<String, String>> getSensesFromVirtuoso(String entityIRI) throws Exception {
        String sparql = String.format("""
            PREFIX : <%s>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT DISTINCT ?senseIRI ?senseLabel ?gloss ?identifier WHERE {
              GRAPH <%s> {
                ?entry :sense <%s> ;
                       :sense ?senseIRI .
                ?senseIRI rdfs:label ?senseLabel .
                OPTIONAL { ?senseIRI :gloss ?gloss . }
                OPTIONAL { ?senseIRI :identifier ?identifier . }
              }
            }
            ORDER BY ?senseLabel
            """, ONT_NS, graphIRI, entityIRI);

        JsonObject json = executeSparqlQuery(sparql);
        if (json == null) return List.of();

        List<Map<String, String>> senses = new ArrayList<>();
        JsonArray bindings = json.getAsJsonObject("results").getAsJsonArray("bindings");

        for (JsonElement el : bindings) {
            JsonObject row = el.getAsJsonObject();
            String senseIRI = getVal(row, "senseIRI");
            Map<String, String> sense = new LinkedHashMap<>();
            sense.put("senseIRI", senseIRI);
            sense.put("label", getVal(row, "senseLabel"));

            String senseId = senseIRI;
            int pos = Math.max(senseIRI.lastIndexOf('#'), senseIRI.lastIndexOf('/'));
            if (pos >= 0) senseId = senseIRI.substring(pos + 1);
            sense.put("senseId", senseId);

            String gloss = getVal(row, "gloss");
            if (gloss != null) sense.put("gloss", gloss);
            String ident = getVal(row, "identifier");
            if (ident != null) sense.put("identifier", ident);

            senses.add(sense);
        }

        return senses;
    }

    /**
     * Index statistics.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("ready", ready);
        stats.put("sparqlEndpoint", sparqlEndpoint);
        stats.put("graphIRI", graphIRI);
        stats.put("entityCounts", new LinkedHashMap<>(entityCounts));
        stats.put("entryCounts", new LinkedHashMap<>(entryCounts));
        stats.put("totalSenses", totalSenses);
        stats.put("buildTimeMs", buildTimeMs);
        stats.put("categories", CATEGORIES);
        return stats;
    }

    public boolean isReady() {
        return ready;
    }

    // ── SPARQL HTTP client ────────────────────────────────────────────────────

    private JsonObject executeSparqlQuery(String sparql) throws Exception {
        String encoded = URLEncoder.encode(sparql, StandardCharsets.UTF_8);
        String url = sparqlEndpoint + "?query=" + encoded + "&format=json";

        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Accept", "application/sparql-results+json")
            .GET()
            .timeout(java.time.Duration.ofSeconds(120))
            .build();

        HttpResponse<String> resp = http.send(req, BodyHandlers.ofString());

        if (resp.statusCode() != 200) {
            System.err.println("[entity-suggest] SPARQL error " + resp.statusCode()
                + ": " + resp.body().substring(0, Math.min(300, resp.body().length())));
            return null;
        }

        return gson.fromJson(resp.body(), JsonObject.class);
    }

    private static String getVal(JsonObject row, String varName) {
        JsonObject binding = row.getAsJsonObject(varName);
        if (binding == null) return null;
        JsonElement val = binding.get("value");
        return val != null ? val.getAsString() : null;
    }

    // ── FST data structures ──────────────────────────────────────────────────

    private static class SuggestEntry {
        final String searchText;
        final String primaryLabel;
        final String iri;
        final String category;
        final String gloss;

        SuggestEntry(String searchText, String primaryLabel, String iri,
                     String category, String gloss) {
            this.searchText = searchText;
            this.primaryLabel = primaryLabel;
            this.iri = iri;
            this.category = category;
            this.gloss = gloss;
        }
    }

    /**
     * Iterator adapter for AnalyzingInfixSuggester.build().
     * Payload: "primaryLabel\tiri\tgloss" — resolved at suggest time, no second lookup.
     */
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
            String gloss = current.gloss != null ? current.gloss : "";
            // Truncate gloss in payload to save FST memory (full gloss is in sense index)
            if (gloss.length() > 120) gloss = gloss.substring(0, 120) + "...";
            return new BytesRef(current.primaryLabel + "\t" + current.iri + "\t" + gloss);
        }

        @Override
        public boolean hasPayloads() { return true; }

        @Override
        public long weight() { return 1; }

        @Override
        public boolean hasContexts() { return false; }

        @Override
        public Set<BytesRef> contexts() { return null; }
    }
}