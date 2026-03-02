package com.example;

import org.apache.jena.rdf.model.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * High-performance entity autocomplete using Lucene's AnalyzingInfixSuggester (FST-backed).
 *
 * Design goals:
 * - Sub-10ms suggest latency for millions of entities
 * - Infix matching ("Abdul La" matches "Abdullah Lateef" and "Steve Abdul-Latif")
 * - Category-scoped queries (Person, Organization, GeopoliticalEntity, Product)
 * - Sense resolution: each entity may link to multiple WordNet/ontology senses
 *
 * Index structure per category:
 *   - AnalyzingInfixSuggester for blazing prefix/infix autocomplete
 *   - Auxiliary Lucene index for sense lookups by IRI
 *
 * Data flow:
 *   1. At startup, scan entity TTL partitions from the manifest
 *   2. Build per-category FST suggesters + sense index
 *   3. Serve /api/entity-suggest and /api/entity-senses endpoints
 */
public class EntitySuggestService {

    private static final String ONT_NS = "https://falcontologist.github.io/shacl-demo/ontology/";

    // Supported entity categories — must match rdf:type local names in TTL
    // The TTL uses :Person_Entity, :Organization_Entity, etc.
    public static final List<String> CATEGORIES = List.of(
        "Person_Entity", "Organization_Entity", "Geopolitical_Entity", "Product_Entity"
    );

    // Corresponding entry classes that hold :sense links
    public static final List<String> ENTRY_CLASSES = List.of(
        "Person_Entry", "Organization_Entry", "Geopolitical_Entry", "Product_Entry"
    );

    // User-friendly display names (keyed by entity class)
    public static final Map<String, String> CATEGORY_LABELS = Map.of(
        "Person_Entity", "Person",
        "Organization_Entity", "Organization",
        "Geopolitical_Entity", "Geopolitical Entity",
        "Product_Entity", "Product"
    );

    // Per-category FST suggester for autocomplete
    private final Map<String, AnalyzingInfixSuggester> suggesters = new ConcurrentHashMap<>();

    // Shared Lucene index for sense lookups (IRI → senses)
    private DirectoryReader senseReader;
    private IndexSearcher senseSearcher;

    // Stats
    private final Map<String, Long> entityCounts = new ConcurrentHashMap<>();
    private long totalSenses = 0;
    private volatile boolean ready = false;

    /**
     * Build the suggest indices from the in-memory ontology model.
     * Call this once at startup after the ontology is loaded.
     *
     * For very large datasets (millions), this scans the model for entities
     * of each category and feeds them into per-category AnalyzingInfixSuggesters.
     */
    public void buildIndex(Model ontologyModel) throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("[entity-suggest] Building autocomplete indices...");

        Property typeProp  = ontologyModel.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        Property labelProp = ontologyModel.createProperty("http://www.w3.org/2000/01/rdf-schema#label");
        Property altLabel  = ontologyModel.createProperty(ONT_NS + "altLabel");
        Property senseProp = ontologyModel.createProperty(ONT_NS + "sense");
        Property glossProp = ontologyModel.createProperty(ONT_NS + "gloss");
        Property identProp = ontologyModel.createProperty(ONT_NS + "identifier");

        // --- Build sense index (shared across all categories) ---
        ByteBuffersDirectory senseDir = new ByteBuffersDirectory();
        IndexWriterConfig senseConfig = new IndexWriterConfig(new StandardAnalyzer());
        senseConfig.setRAMBufferSizeMB(64);
        IndexWriter senseWriter = new IndexWriter(senseDir, senseConfig);

        // --- Process each category pair ---
        for (int ci = 0; ci < CATEGORIES.size(); ci++) {
            String entityClass = CATEGORIES.get(ci);
            String entryClass  = ENTRY_CLASSES.get(ci);
            String displayName = CATEGORY_LABELS.getOrDefault(entityClass, entityClass);

            System.out.println("[entity-suggest]   Indexing: " + displayName 
                + " (" + entityClass + " / " + entryClass + ")");

            Resource entityTypeRes = ontologyModel.createResource(ONT_NS + entityClass);
            Resource entryTypeRes  = ontologyModel.createResource(ONT_NS + entryClass);

            // ── Step 1: Pre-cache entity data (gloss, label, identifier) by IRI ──
            // Entities ARE the senses. Pattern:
            //   :Spanish_wine.product_entity.01 a :Product_Entity ;
            //       rdfs:label "Spanish wine (entity)"@en ;
            //       :gloss "wines of Spain"@en ;
            //       :identifier wiki:Q1432594 .
            Map<String, EntityInfo> entityInfoMap = new HashMap<>();

            ontologyModel.listSubjectsWithProperty(typeProp, entityTypeRes).forEachRemaining(entity -> {
                String iri = entity.getURI();
                if (iri == null) return;

                String label = null;
                Statement labelSt = entity.getProperty(labelProp);
                if (labelSt != null && labelSt.getObject().isLiteral()) {
                    label = labelSt.getObject().asLiteral().getString();
                }

                String gloss = null;
                Statement glossSt = entity.getProperty(glossProp);
                if (glossSt != null && glossSt.getObject().isLiteral()) {
                    gloss = glossSt.getObject().asLiteral().getString();
                }

                String identifier = null;
                Statement identSt = entity.getProperty(identProp);
                if (identSt != null) {
                    identifier = identSt.getObject().isResource() 
                        ? identSt.getObject().asResource().getURI()
                        : identSt.getObject().asLiteral().getString();
                }

                // Collect alt labels
                Set<String> altLabels = new LinkedHashSet<>();
                entity.listProperties(altLabel).forEachRemaining(stmt -> {
                    if (stmt.getObject().isLiteral()) {
                        altLabels.add(stmt.getObject().asLiteral().getString());
                    }
                });

                entityInfoMap.put(iri, new EntityInfo(iri, label, gloss, identifier, altLabels));
            });

            System.out.println("[entity-suggest]     Entities cached: " + entityInfoMap.size());

            // ── Step 2: Scan entries to build suggest list + sense index ──
            // Entries link to entities via :sense. Pattern:
            //   :multifunction_printer.product_entry a :Product_Entry ;
            //       rdfs:label "multifunction printer"@en ;
            //       :sense :multifunction_printer.product_entity.01 .
            //
            // The entry's rdfs:label is the search text.
            // The :sense target is the entity IRI (which has :gloss).
            // An entry can have multiple :sense values (multiple entity matches).

            AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(
                new ByteBuffersDirectory(),
                new StandardAnalyzer()
            );

            // We'll collect suggest entries keyed by entry label.
            // Multiple entries may share a label; we deduplicate by entity IRI.
            Map<String, Set<String>> labelToEntityIRIs = new LinkedHashMap<>();
            List<SuggestEntry> suggestEntries = new ArrayList<>();
            AtomicLong entryCount = new AtomicLong(0);

            ontologyModel.listSubjectsWithProperty(typeProp, entryTypeRes).forEachRemaining(entry -> {
                // Get entry label (the search surface form)
                Statement labelSt = entry.getProperty(labelProp);
                if (labelSt == null || !labelSt.getObject().isLiteral()) return;
                String entryLabel = labelSt.getObject().asLiteral().getString();

                // Collect all :sense targets (entity IRIs)
                List<String> senseTargets = new ArrayList<>();
                entry.listProperties(senseProp).forEachRemaining(senseStmt -> {
                    if (senseStmt.getObject().isResource()) {
                        senseTargets.add(senseStmt.getObject().asResource().getURI());
                    }
                });

                if (senseTargets.isEmpty()) return;

                // Use the first sense target's entity as the "primary" for the suggest payload.
                // The user will pick the specific sense in the second dropdown.
                String primaryEntityIRI = senseTargets.get(0);
                EntityInfo primaryInfo = entityInfoMap.get(primaryEntityIRI);
                String displayLabel = primaryInfo != null && primaryInfo.label != null 
                    ? primaryInfo.label : entryLabel;

                // Add suggest entry (entry label → primary entity IRI)
                suggestEntries.add(new SuggestEntry(entryLabel, displayLabel, primaryEntityIRI, entityClass));

                // Index sense mappings: for this entry label, record all entity targets
                // so /api/entity-senses can return them
                for (String entityIRI : senseTargets) {
                    EntityInfo info = entityInfoMap.get(entityIRI);
                    if (info == null) continue;

                    Document doc = new Document();
                    // Key: the entry's primary entity IRI (what the suggest returns)
                    doc.add(new StringField("entityIRI", primaryEntityIRI, Field.Store.YES));
                    doc.add(new StringField("senseIRI", entityIRI, Field.Store.YES));
                    doc.add(new StringField("senseId", info.localName(), Field.Store.YES));
                    doc.add(new StoredField("gloss", info.gloss != null ? info.gloss : ""));
                    doc.add(new StoredField("label", info.label != null ? info.label : ""));
                    doc.add(new StringField("category", entityClass, Field.Store.YES));
                    if (info.identifier != null) {
                        doc.add(new StoredField("identifier", info.identifier));
                    }

                    try {
                        senseWriter.addDocument(doc);
                    } catch (IOException e) {
                        System.err.println("[entity-suggest] Sense index error: " + e.getMessage());
                    }
                }

                entryCount.incrementAndGet();
            });

            // Also add suggest entries directly from entities that may not have entries
            // (e.g., entities with altLabels we want searchable)
            for (EntityInfo info : entityInfoMap.values()) {
                // Add altLabel variants as additional suggest entries
                for (String alt : info.altLabels) {
                    suggestEntries.add(new SuggestEntry(alt, 
                        info.label != null ? info.label : alt, info.iri, entityClass));
                }
            }

            // Build the FST suggester
            if (!suggestEntries.isEmpty()) {
                suggester.build(new SuggestEntryIterator(suggestEntries));
                System.out.println("[entity-suggest]     " + displayName + ": " 
                    + entryCount.get() + " entries, " 
                    + entityInfoMap.size() + " entities, "
                    + suggestEntries.size() + " suggest variants");
            } else {
                System.out.println("[entity-suggest]     " + displayName + ": 0 entries (empty)");
            }

            suggesters.put(entityClass, suggester);
            entityCounts.put(entityClass, entryCount.get());
        }

        // Finalize sense index
        senseWriter.commit();
        senseWriter.close();
        senseReader = DirectoryReader.open(senseDir);
        senseSearcher = new IndexSearcher(senseReader);
        totalSenses = senseReader.numDocs();

        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("[entity-suggest] Index build complete in " + elapsed + "ms");
        System.out.println("[entity-suggest]   Total entries indexed: " + 
            entityCounts.values().stream().mapToLong(Long::longValue).sum());
        System.out.println("[entity-suggest]   Total sense documents: " + totalSenses);

        ready = true;
    }

    /**
     * Autocomplete suggest: returns top matches for a query within a category.
     *
     * @param category One of: Person, Organization, GeopoliticalEntity, Product
     * @param query    Partial text (e.g. "Abdul La")
     * @param limit    Max results (default 10)
     * @return List of {label, iri, category} maps
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
            // payload format: "primaryLabel\tiri"
            String[] parts = payload.split("\t", 2);
            if (parts.length < 2) continue;

            String primaryLabel = parts[0];
            String iri = parts[1];
            String matchedText = result.key.toString();

            if (!deduped.containsKey(iri)) {
                Map<String, String> entry = new LinkedHashMap<>();
                entry.put("label", primaryLabel);
                entry.put("iri", iri);
                entry.put("category", category);
                // If the match was on an altLabel, show it
                if (!matchedText.equals(primaryLabel)) {
                    entry.put("matchedLabel", matchedText);
                }
                deduped.put(iri, entry);
            }
        }

        return new ArrayList<>(deduped.values());
    }

    /**
     * Get all senses for a given entity IRI.
     *
     * @param entityIRI The full IRI of the entity
     * @return List of {senseId, senseIRI, gloss} maps
     */
    public List<Map<String, String>> getSenses(String entityIRI) throws Exception {
        if (!ready || senseSearcher == null) return List.of();

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
            String ident = doc.get("identifier");
            if (ident != null) sense.put("identifier", ident);
            senses.add(sense);
        }

        return senses;
    }

    /**
     * Get index statistics.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("ready", ready);
        stats.put("entityCounts", new LinkedHashMap<>(entityCounts));
        stats.put("totalSenses", totalSenses);
        stats.put("categories", CATEGORIES);
        return stats;
    }

    public boolean isReady() {
        return ready;
    }

    // ========================================================================
    // Internal: EntityInfo — cached entity data for linking
    // ========================================================================

    private static class EntityInfo {
        final String iri;
        final String label;
        final String gloss;
        final String identifier;
        final Set<String> altLabels;

        EntityInfo(String iri, String label, String gloss, String identifier, Set<String> altLabels) {
            this.iri = iri;
            this.label = label;
            this.gloss = gloss;
            this.identifier = identifier;
            this.altLabels = altLabels != null ? altLabels : Set.of();
        }

        /** Extract local name from IRI (e.g. "Spanish_wine.product_entity.01") */
        String localName() {
            if (iri == null) return "";
            int hash = iri.lastIndexOf('#');
            int slash = iri.lastIndexOf('/');
            int pos = Math.max(hash, slash);
            return pos >= 0 ? iri.substring(pos + 1) : iri;
        }
    }

    // ========================================================================
    // Internal: SuggestEntry + Iterator for AnalyzingInfixSuggester.build()
    // ========================================================================

    private static class SuggestEntry {
        final String searchText;   // The text to match against (may be altLabel)
        final String primaryLabel; // The primary display label
        final String iri;
        final String category;

        SuggestEntry(String searchText, String primaryLabel, String iri, String category) {
            this.searchText = searchText;
            this.primaryLabel = primaryLabel;
            this.iri = iri;
            this.category = category;
        }
    }

    /**
     * Iterator adapter for feeding entries into AnalyzingInfixSuggester.
     * Payload encodes "primaryLabel\tiri" for retrieval at suggest time.
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
            return new BytesRef(current.primaryLabel + "\t" + current.iri);
        }

        @Override
        public boolean hasPayloads() {
            return true;
        }

        @Override
        public long weight() {
            // Could use popularity/frequency ranking here.
            // For now, uniform weight; Lucene still ranks by match quality.
            return 1;
        }

        @Override
        public boolean hasContexts() {
            return false;
        }

        @Override
        public Set<BytesRef> contexts() {
            return null;
        }
    }
}