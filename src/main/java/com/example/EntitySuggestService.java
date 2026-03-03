package com.example;

import com.google.gson.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.suggest.Lookup;
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
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;

/**
 * Entity autocomplete backed by a pre-built Lucene FST index.
 *
 * Architecture:
 *   - At startup, downloads fst-index.tar.gz from GCS (~50-100MB compressed)
 *   - Opens per-category AnalyzingInfixSuggester from disk (FSDirectory)
 *   - Opens shared sense index for IRI→sense lookups
 *   - Runtime suggest hits local Lucene (sub-10ms), zero Virtuoso queries
 *   - Sense resolution falls back to Virtuoso SPARQL if not in local index
 *
 * Build the index locally with BuildFSTIndex.java, upload to GCS.
 */
public class EntitySuggestService {

    private static final String ONT_NS = "https://falcontologist.github.io/shacl-demo/ontology/";

    // GCS URL for the pre-built index archive
    private static final String FST_INDEX_URL =
        System.getenv().getOrDefault("FST_INDEX_URL",
            "https://storage.googleapis.com/fkg/fst-index.tar.gz");

    public static final List<String> CATEGORIES = List.of(
        "Person_Entity", "Organization_Entity", "Geopolitical_Entity", "Product_Entity"
    );

    public static final Map<String, String> ENTRY_CLASSES = Map.of(
        "Person_Entity", "Person_Entry",
        "Organization_Entity", "Organization_Entry",
        "Geopolitical_Entity", "Geopolitical_Entry",
        "Product_Entity", "Product_Entry"
    );

    // Per-category FST suggester
    private final Map<String, AnalyzingInfixSuggester> suggesters = new ConcurrentHashMap<>();

    // Shared sense index
    private DirectoryReader senseReader;
    private IndexSearcher senseSearcher;

    // Virtuoso fallback for sense lookups
    private final String sparqlEndpoint;
    private final String graphIRI;
    private final HttpClient http;
    private final Gson gson;

    // Stats
    private final Map<String, Long> entityCounts = new ConcurrentHashMap<>();
    private final Map<String, Long> entryCounts = new ConcurrentHashMap<>();
    private long totalSenses = 0;
    private volatile boolean ready = false;
    private long loadTimeMs = 0;

    // Local path for extracted index
    private static final Path INDEX_DIR = Path.of("/tmp/fst-index");

    public EntitySuggestService(String sparqlEndpoint, String graphIRI) {
        this.sparqlEndpoint = sparqlEndpoint;
        this.graphIRI = graphIRI;
        this.http = HttpClient.newHttpClient();
        this.gson = new Gson();
    }

    /**
     * Download and load the pre-built FST index from GCS.
     * Much faster than building from SPARQL (~10s vs ~5min) and uses minimal memory.
     */
    public void loadPrebuiltIndex() throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("[entity-suggest] Loading pre-built FST index from GCS...");
        System.out.println("[entity-suggest] URL: " + FST_INDEX_URL);

        // Download and extract
        Path archivePath = Path.of("/tmp/fst-index.tar.gz");
        downloadFile(FST_INDEX_URL, archivePath);
        extractTarGz(archivePath, INDEX_DIR);

        // Load stats
        Path statsFile = INDEX_DIR.resolve("stats.json");
        if (Files.exists(statsFile)) {
            String statsJson = Files.readString(statsFile);
            JsonObject stats = gson.fromJson(statsJson, JsonObject.class);

            JsonObject ec = stats.getAsJsonObject("entityCounts");
            if (ec != null) {
                ec.entrySet().forEach(e ->
                    entityCounts.put(e.getKey(), e.getValue().getAsLong()));
            }
            JsonObject enc = stats.getAsJsonObject("entryCounts");
            if (enc != null) {
                enc.entrySet().forEach(e ->
                    entryCounts.put(e.getKey(), e.getValue().getAsLong()));
            }
            System.out.println("[entity-suggest] Stats loaded: " + entityCounts);
        }

        // Open per-category FST suggesters
        for (String category : CATEGORIES) {
            Path fstDir = INDEX_DIR.resolve("fst-" + category);
            if (Files.exists(fstDir) && Files.list(fstDir).findAny().isPresent()) {
                AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(
                    FSDirectory.open(fstDir),
                    new StandardAnalyzer()
                );
                suggesters.put(category, suggester);
                System.out.println("[entity-suggest]   " + category + ": loaded ("
                    + entityCounts.getOrDefault(category, 0L) + " entities)");
            } else {
                System.out.println("[entity-suggest]   " + category + ": not found, skipping");
            }
        }

        // Open sense index
        Path senseDir = INDEX_DIR.resolve("sense-index");
        if (Files.exists(senseDir)) {
            senseReader = DirectoryReader.open(FSDirectory.open(senseDir));
            senseSearcher = new IndexSearcher(senseReader);
            totalSenses = senseReader.numDocs();
            System.out.println("[entity-suggest]   Sense index: " + totalSenses + " documents");
        }

        loadTimeMs = System.currentTimeMillis() - startTime;
        ready = true;

        System.out.println("[entity-suggest] Index loaded in " + loadTimeMs + "ms");
        System.out.println("[entity-suggest]   Total entities: "
            + entityCounts.values().stream().mapToLong(Long::longValue).sum());

        // Clean up archive
        Files.deleteIfExists(archivePath);
    }

    // ── Download + Extract ────────────────────────────────────────────────────

    private void downloadFile(String url, Path target) throws Exception {
        System.out.println("[entity-suggest]   Downloading " + url + " ...");
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .GET()
            .timeout(java.time.Duration.ofSeconds(300))
            .build();

        HttpResponse<Path> resp = http.send(req,
            HttpResponse.BodyHandlers.ofFile(target));

        if (resp.statusCode() != 200) {
            throw new IOException("Download failed: HTTP " + resp.statusCode());
        }

        long size = Files.size(target);
        System.out.printf("[entity-suggest]   Downloaded: %,d bytes%n", size);
    }

    private void extractTarGz(Path archive, Path targetDir) throws Exception {
        System.out.println("[entity-suggest]   Extracting to " + targetDir + " ...");

        // Clean target directory
        if (Files.exists(targetDir)) {
            try (var walk = Files.walk(targetDir)) {
                walk.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            }
        }
        Files.createDirectories(targetDir);

        // Use system tar for efficiency
        ProcessBuilder pb = new ProcessBuilder("tar", "-xzf", archive.toString(), "-C", targetDir.toString());
        pb.inheritIO();
        Process proc = pb.start();
        int exitCode = proc.waitFor();
        if (exitCode != 0) {
            throw new IOException("tar extraction failed with exit code " + exitCode);
        }

        long fileCount;
        try (var walk = Files.walk(targetDir)) {
            fileCount = walk.filter(Files::isRegularFile).count();
        }
        System.out.println("[entity-suggest]   Extracted " + fileCount + " files");
    }

    // ── Suggest ───────────────────────────────────────────────────────────────

    /**
     * Sub-10ms autocomplete via Lucene FST.
     */
    public List<Map<String, String>> suggest(String category, String query, int limit) throws Exception {
        if (!ready) return List.of();

        AnalyzingInfixSuggester suggester = suggesters.get(category);
        if (suggester == null) return List.of();

        List<Lookup.LookupResult> results = suggester.lookup(query, false, limit);

        LinkedHashMap<String, Map<String, String>> deduped = new LinkedHashMap<>();
        for (Lookup.LookupResult result : results) {
            String payload = result.payload != null ? result.payload.utf8ToString() : "";
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

                String localName = iri;
                int pos = Math.max(iri.lastIndexOf('#'), iri.lastIndexOf('/'));
                if (pos >= 0) localName = iri.substring(pos + 1);
                entry.put("localName", localName);

                if (!matchedText.equals(primaryLabel)) {
                    entry.put("matchedLabel", matchedText);
                }
                deduped.put(iri, entry);
            }
        }

        return new ArrayList<>(deduped.values());
    }

    // ── Sense lookup ──────────────────────────────────────────────────────────

    public List<Map<String, String>> getSenses(String entityIRI) throws Exception {
        if (ready && senseSearcher != null) {
            List<Map<String, String>> local = getSensesFromLucene(entityIRI);
            if (!local.isEmpty()) return local;
        }
        return getSensesFromVirtuoso(entityIRI);
    }

    private List<Map<String, String>> getSensesFromLucene(String entityIRI) throws Exception {
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

    // ── Stats ─────────────────────────────────────────────────────────────────

    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("ready", ready);
        stats.put("indexSource", FST_INDEX_URL);
        stats.put("sparqlEndpoint", sparqlEndpoint);
        stats.put("graphIRI", graphIRI);
        stats.put("entityCounts", new LinkedHashMap<>(entityCounts));
        stats.put("entryCounts", new LinkedHashMap<>(entryCounts));
        stats.put("totalSenses", totalSenses);
        stats.put("loadTimeMs", loadTimeMs);
        stats.put("categories", CATEGORIES);
        return stats;
    }

    public boolean isReady() {
        return ready;
    }

    // ── SPARQL client (sense fallback only) ───────────────────────────────────

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
}
