package com.gdblab.graphstorage.storage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// Metadara now works in memory and with JSON for readability
class MetaStore {

    private long nodeCount;
    private long edgeCount;

    private final Map<String, Long> edgeCountByLabel;
    private final Map<String, Set<String>> nodeSchema;
    private final Map<String, Set<String>> edgeSchema;

    private static class MetaData {
        long nodeCount;
        long edgeCount;
        Map<String, Long> edgeCountByLabel;
        Map<String, Set<String>> nodeSchema;
        Map<String, Set<String>> edgeSchema;
    }

    private MetaStore() {
        this.nodeCount = 0;
        this.edgeCount = 0;
        this.edgeCountByLabel = new ConcurrentHashMap<>();
        this.nodeSchema = new ConcurrentHashMap<>();
        this.edgeSchema = new ConcurrentHashMap<>();
    }

    // load metastore from file
    public static MetaStore load(Path dbPath) throws IOException {
        Path metaFile = dbPath.resolve("metadata.json");
        if (!Files.exists(metaFile)) {
            return new MetaStore();
        }

        Gson gson = new Gson();
        try (Reader reader = new FileReader(metaFile.toFile())) {
            MetaData data = gson.fromJson(reader, MetaData.class);
            
            MetaStore metaStore = new MetaStore();
            metaStore.nodeCount = data.nodeCount;
            metaStore.edgeCount = data.edgeCount;
            metaStore.edgeCountByLabel.putAll(data.edgeCountByLabel);
            metaStore.nodeSchema.putAll(data.nodeSchema);
            metaStore.edgeSchema.putAll(data.edgeSchema);
            return metaStore;
        }
    }

     // Save current metastore state in 'metadata.json'
    public void save(Path dbPath) throws IOException {
        Path metaFile = dbPath.resolve("metadata.json");
        
        MetaData data = new MetaData();
        data.nodeCount = this.nodeCount;
        data.edgeCount = this.edgeCount;
        data.edgeCountByLabel = this.edgeCountByLabel;
        data.nodeSchema = this.nodeSchema;
        data.edgeSchema = this.edgeSchema;

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try (FileWriter writer = new FileWriter(metaFile.toFile())) {
            gson.toJson(data, writer);
        }
    }

    // Updating methods

    public void incNodeCount(long delta) {
        this.nodeCount += delta;
    }

    public void incEdgeCount(long delta) {
        this.edgeCount += delta;
    }

    public void incEdgeCountByLabel(String label, long delta) {
        edgeCountByLabel.merge(label, delta, Long::sum);
    }

    public void addNodeLabel(String label) {
        nodeSchema.computeIfAbsent(label, k -> new LinkedHashSet<>());
    }

    public void addNodeProp(String label, String prop) {
        nodeSchema.computeIfAbsent(label, k -> new LinkedHashSet<>()).add(prop);
    }

    public void addEdgeLabel(String label) {
        edgeSchema.computeIfAbsent(label, k -> new LinkedHashSet<>());
    }

    public void addEdgeProp(String label, String prop) {
        edgeSchema.computeIfAbsent(label, k -> new LinkedHashSet<>()).add(prop);
    }

    // Reading methods

    public long getNodeCount() { return this.nodeCount; }
    public long getEdgeCount() { return this.edgeCount; }
    public Map<String, Long> getEdgeCountsByLabel() { return new LinkedHashMap<>(this.edgeCountByLabel); }
    public Map<String, Set<String>> getNodeSchema() { return new LinkedHashMap<>(this.nodeSchema); }
    public Map<String, Set<String>> getEdgeSchema() { return new LinkedHashMap<>(this.edgeSchema); }
}