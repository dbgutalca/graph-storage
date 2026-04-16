package com.gdblab.graphstorage.engine;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MetaStore {

    private long nodeCount;
    private long edgeCount;

    private final Map<String, Long> nodeCountByLabel; 
    
    private final Map<String, Long> edgeCountByLabel;
    private final Map<String, Set<String>> nodeSchema;
    private final Map<String, Set<String>> edgeSchema;

    private final Map<String, Map<EdgeConnection, Long>> edgeConnectionsMap;

    public static record EdgeConnection(String srcLabel, String dstLabel) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EdgeConnection that = (EdgeConnection) o;
            return Objects.equals(srcLabel, that.srcLabel) && Objects.equals(dstLabel, that.dstLabel);
        }
        @Override
        public int hashCode() {
            return Objects.hash(srcLabel, dstLabel);
        }
    }

    public static class EdgeConnectionStats {
        public String srcLabel;
        public String dstLabel;
        public long count;

        public EdgeConnectionStats(String src, String dst, long count) {
            this.srcLabel = src;
            this.dstLabel = dst;
            this.count = count;
        }
    }

    private static class MetaData {
        long nodeCount;
        long edgeCount;
        Map<String, Long> nodeCountByLabel;
        Map<String, Long> edgeCountByLabel;
        Map<String, Set<String>> nodeSchema;
        Map<String, Set<String>> edgeSchema;
        Map<String, List<EdgeConnectionStats>> edgeConnections;
    }

    private MetaStore() {
        this.nodeCount = 0;
        this.edgeCount = 0;
        this.nodeCountByLabel = new ConcurrentHashMap<>();
        this.edgeCountByLabel = new ConcurrentHashMap<>();
        this.nodeSchema = new ConcurrentHashMap<>();
        this.edgeSchema = new ConcurrentHashMap<>();
        this.edgeConnectionsMap = new ConcurrentHashMap<>();
    }

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
            
            if (data.nodeCountByLabel != null) metaStore.nodeCountByLabel.putAll(data.nodeCountByLabel);
            if (data.edgeCountByLabel != null) metaStore.edgeCountByLabel.putAll(data.edgeCountByLabel);
            if (data.nodeSchema != null) metaStore.nodeSchema.putAll(data.nodeSchema);
            if (data.edgeSchema != null) metaStore.edgeSchema.putAll(data.edgeSchema);

            if (data.edgeConnections != null) {
                data.edgeConnections.forEach((label, list) -> {
                    Map<EdgeConnection, Long> innerMap = new ConcurrentHashMap<>();
                    for (EdgeConnectionStats stats : list) {
                        innerMap.put(new EdgeConnection(stats.srcLabel, stats.dstLabel), stats.count);
                    }
                    metaStore.edgeConnectionsMap.put(label, innerMap);
                });
            }

            return metaStore;
        }
    }

    public void save(Path dbPath) throws IOException {
        Path metaFile = dbPath.resolve("metadata.json");
        
        MetaData data = new MetaData();
        data.nodeCount = this.nodeCount;
        data.edgeCount = this.edgeCount;
        data.nodeCountByLabel = this.nodeCountByLabel;
        data.edgeCountByLabel = this.edgeCountByLabel;
        data.nodeSchema = this.nodeSchema;
        data.edgeSchema = this.edgeSchema;

        data.edgeConnections = new HashMap<>();
        this.edgeConnectionsMap.forEach((label, map) -> {
            List<EdgeConnectionStats> list = map.entrySet().stream()
                .map(e -> new EdgeConnectionStats(e.getKey().srcLabel(), e.getKey().dstLabel(), e.getValue()))
                .collect(Collectors.toList());
            data.edgeConnections.put(label, list);
        });

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try (FileWriter writer = new FileWriter(metaFile.toFile())) {
            gson.toJson(data, writer);
        }
    }

    public void incNodeCount(long delta) { this.nodeCount += delta; }
    public void incEdgeCount(long delta) { this.edgeCount += delta; }

    public void incNodeCountByLabel(String label, long delta) {
        nodeCountByLabel.merge(label, delta, Long::sum);
    }

    public void incEdgeCountByLabel(String label, long delta) {
        edgeCountByLabel.merge(label, delta, Long::sum);
    }

    public void incEdgeConnection(String edgeLabel, String srcLabel, String dstLabel, long delta) {
        if (srcLabel == null || dstLabel == null) return;
        
        edgeConnectionsMap
            .computeIfAbsent(edgeLabel, k -> new ConcurrentHashMap<>())
            .merge(new EdgeConnection(srcLabel, dstLabel), delta, Long::sum);
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


    public long getNodeCount() { return this.nodeCount; }
    public long getEdgeCount() { return this.edgeCount; }
    public Map<String, Long> getEdgeCountsByLabel() { return new LinkedHashMap<>(this.edgeCountByLabel); }
    public Map<String, Set<String>> getNodeSchema() { return new LinkedHashMap<>(this.nodeSchema); }
    public Map<String, Set<String>> getEdgeSchema() { return new LinkedHashMap<>(this.edgeSchema); }
    
    public Map<String, List<EdgeConnectionStats>> getEdgeConnections() {
        Map<String, List<EdgeConnectionStats>> result = new LinkedHashMap<>();
        this.edgeConnectionsMap.forEach((label, map) -> {
            List<EdgeConnectionStats> list = map.entrySet().stream()
                .map(e -> new EdgeConnectionStats(e.getKey().srcLabel(), e.getKey().dstLabel(), e.getValue()))
                .collect(Collectors.toList());
            result.put(label, list);
        });
        return result;
    }

    public String toJsonString() {
        MetaData data = new MetaData();
        data.nodeCount = this.nodeCount;
        data.edgeCount = this.edgeCount;
        data.nodeCountByLabel = this.nodeCountByLabel;
        data.edgeCountByLabel = this.edgeCountByLabel;
        data.nodeSchema = this.nodeSchema;
        data.edgeSchema = this.edgeSchema;
        
        data.edgeConnections = new HashMap<>();
        this.edgeConnectionsMap.forEach((label, map) -> {
            List<EdgeConnectionStats> list = map.entrySet().stream()
                .map(e -> new EdgeConnectionStats(e.getKey().srcLabel(), e.getKey().dstLabel(), e.getValue()))
                .collect(Collectors.toList());
            data.edgeConnections.put(label, list);
        });

        return new Gson().toJson(data);
    }
}
