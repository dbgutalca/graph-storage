package com.gdblab.graphstorage.storage;


import org.rocksdb.RocksDBException;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;


public class GraphIngestor implements Closeable {
    private final NodeStore nodeStore;
    private final EdgeStore edgeStore;
    private final IndexStore indexStore;
    private final MetaStore metaStore;
    
    GraphIngestor(NodeStore nodeStore, EdgeStore edgeStore, IndexStore indexStore, MetaStore metaStore) {
        this.nodeStore = nodeStore;
        this.edgeStore = edgeStore;
        this.indexStore = indexStore;
        this.metaStore = metaStore;
    }

    /** nodes.pgdf */
    public void ingestNodes(Path nodesPgdf) throws IOException, RocksDBException {
        try (BufferedReader br = Files.newBufferedReader(nodesPgdf, StandardCharsets.UTF_8)) {
            String line; String[] header = null;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                if (line.startsWith("@")) {
                    header = Arrays.stream(line.split("\\|"))
                            .map(String::trim)
                            .toArray(String[]::new);
                    continue;
                }
                if (header == null) continue;

                String[] cols = line.split("\\|", -1);
                Map<String,String> row = new LinkedHashMap<>();
                for (int i=0;i<header.length && i<cols.length;i++) row.put(header[i], cols[i]);

                String nodeId = row.getOrDefault("@id","" ).trim();
                String label  = row.getOrDefault("@label","" ).trim();
                if (nodeId.isEmpty() || label.isEmpty()) continue;

                Map<String,String> props = new LinkedHashMap<>();
                for (var e : row.entrySet()){
                    String k = e.getKey(); if (k.startsWith("@")) continue; 
                    String v = e.getValue()==null? "" : e.getValue();
                    props.put(k, v);
                }

                nodeStore.put(nodeId, label, props);

                for (var e : props.entrySet()){
                    if (e.getValue()==null || e.getValue().isEmpty()) continue;
                    indexStore.putNodePropEq(e.getKey(), e.getValue(), nodeId);
                }

                metaStore.incNodeCount(1);
                metaStore.addNodeLabel(label);
                for (String p : props.keySet()) {
                    metaStore.addNodeProp(label, p);
                }
            }
        }
    }

    public void ingestEdges(Path edgesPgdf) throws IOException, RocksDBException {
        try (BufferedReader br = Files.newBufferedReader(edgesPgdf, StandardCharsets.UTF_8)) {
            String line; String[] header = null;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                if (line.startsWith("@")) {
                    header = Arrays.stream(line.split("\\|"))
                            .map(String::trim)
                            .toArray(String[]::new);
                    continue;
                }
                if (header == null) continue;

                String[] cols = line.split("\\|", -1);
                Map<String,String> row = new LinkedHashMap<>();
                for (int i=0;i<header.length && i<cols.length;i++) row.put(header[i], cols[i]);

                String edgeId = row.getOrDefault("@id","" ).trim();
                String label  = row.getOrDefault("@label","" ).trim();
                String dir    = row.getOrDefault("@dir","T").trim();
                String src    = row.getOrDefault("@out","" ).trim();
                String dst    = row.getOrDefault("@in",""  ).trim();
                if (label.isEmpty() || src.isEmpty() || dst.isEmpty()) continue;
                if (!"T".equalsIgnoreCase(dir)) continue; 

                if (edgeId.isEmpty()) edgeId = KeySchema.makeEdgeId(src, label, dst);

                Map<String,String> props = new LinkedHashMap<>();
                for (var e : row.entrySet()){
                    String k = e.getKey();
                    if (k.startsWith("@")) continue;
                    String v = e.getValue()==null? "" : e.getValue();
                    props.put(k, v);
                }

                edgeStore.put(edgeId, label, src, dst, props);

                indexStore.putEdgeLabel(label, edgeId);
                indexStore.putSrcNodeByLabel(label, src);
                indexStore.putDstNodeByLabel(label, dst);

                for (var e : props.entrySet()){
                    if (e.getValue()==null || e.getValue().isEmpty()) continue;
                    indexStore.putEdgePropEq(e.getKey(), e.getValue(), edgeId);
                }
                indexStore.putEdgeBySrc(src, edgeId);
                indexStore.putEdgeByDst(dst, edgeId);

                metaStore.incEdgeCount(1);
                metaStore.incEdgeCountByLabel(label, 1);
                metaStore.addEdgeLabel(label);
                for (String p : props.keySet()) {
                    metaStore.addEdgeProp(label, p);
                }
            }
        }
    }

    @Override public void close() throws IOException { }
}
