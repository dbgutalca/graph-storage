package com.gdblab.graphstorage.storage;

import org.rocksdb.*;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class GraphIngestor implements Closeable {
    
    private final RocksDB db;
    private final ColumnFamilyHandle cfNodes;
    private final ColumnFamilyHandle cfEdges;
    private final ColumnFamilyHandle cfIndex;
    private final MetaStore metaStore;

    private Map<String, String> nodeIdToLabelCache;

    // Constants for batching insertion
    // 16MB (16 * 1024 * 1024 bytes)
    private static final long MAX_BATCH_SIZE_BYTES = 16_777_216L; 
    private static final byte[] EMPTY_VALUE = new byte[0];

    GraphIngestor(RocksDB db,
                    ColumnFamilyHandle cfNodes,
                    ColumnFamilyHandle cfEdges,
                    ColumnFamilyHandle cfIndex,
                    MetaStore metaStore) {
        this.db = db;
        this.cfNodes = cfNodes;
        this.cfEdges = cfEdges;
        this.cfIndex = cfIndex;
        this.metaStore = metaStore;

        this.nodeIdToLabelCache = new HashMap<>();
    }

    public void ingestNodes(Path nodesPgdf) throws IOException, RocksDBException {

        WriteOptions writeOptions = new WriteOptions(); 
        WriteBatch batch = new WriteBatch(); 
        long currentBatchBytes = 0; 

        // counters for metastore
        long batchNodeCount = 0;
        Set<String> batchNodeLabels = new HashSet<>();
        Map<String, Set<String>> batchNodeProps = new HashMap<>();

        try (BufferedReader br = Files.newBufferedReader(nodesPgdf, StandardCharsets.UTF_8)) {
            String line; String[] header = null;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                if (line.startsWith("@")) {
                    header = Arrays.stream(line.split("\\|")).map(String::trim).toArray(String[]::new);
                    continue;
                }
                if (header == null) continue;

                //parsing
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

                // Save id -> label in cache
                nodeIdToLabelCache.put(nodeId, label);

                //  Prepare lbytes fot batch insert
                byte[] nodeKey = KeySchema.keyNode(nodeId);
                byte[] nodeValue = NodeBlob.encode(label, props);

                // add to batch
                batch.put(cfNodes, nodeKey, nodeValue);
                currentBatchBytes += nodeKey.length + nodeValue.length;

                // índexes to the batch
                for (var e : props.entrySet()){
                    if (e.getValue()==null || e.getValue().isEmpty()) continue;
                    String normValue = KeySchema.norm(e.getValue());
                    byte[] indexKey = KeySchema.idxKey("prop", e.getKey(), normValue, nodeId);
                    
                    batch.put(cfIndex, indexKey, EMPTY_VALUE);
                    currentBatchBytes += indexKey.length;
                }
                
                //counters
                batchNodeCount++;
                batchNodeLabels.add(label);
                Set<String> propsForLabel = batchNodeProps.computeIfAbsent(label, k -> new HashSet<>());
                propsForLabel.addAll(props.keySet());

                // write when the batch is bigger than the limit
                if (currentBatchBytes > MAX_BATCH_SIZE_BYTES) {
                    db.write(writeOptions, batch);
                    
                    //update metastore
                    metaStore.incNodeCount(batchNodeCount);
                    batchNodeLabels.forEach(metaStore::addNodeLabel);
                    batchNodeProps.forEach((lbl, propSet) -> 
                        propSet.forEach(p -> metaStore.addNodeProp(lbl, p))
                    );

                    // start again 
                    batch.close(); 
                    batch = new WriteBatch();
                    currentBatchBytes = 0;
                    batchNodeCount = 0;
                    batchNodeLabels.clear();
                    batchNodeProps.clear();
                }
            } 
            
            // write the last batch
            if (batchNodeCount > 0) {
                db.write(writeOptions, batch);
                
                metaStore.incNodeCount(batchNodeCount);
                batchNodeLabels.forEach(metaStore::addNodeLabel);
                batchNodeProps.forEach((lbl, propSet) -> 
                    propSet.forEach(p -> metaStore.addNodeProp(lbl, p))
                );
            }

        } finally {
            batch.close();
            writeOptions.close();
        }
    }

    public void ingestEdges(Path edgesPgdf) throws IOException, RocksDBException {
        
        WriteOptions writeOptions = new WriteOptions();
        WriteBatch batch = new WriteBatch();
        long currentBatchBytes = 0;

        long batchEdgeCount = 0;
        Map<String, Long> batchEdgeCountByLabel = new HashMap<>();
        Map<String, Set<String>> batchEdgeProps = new HashMap<>();

        try (BufferedReader br = Files.newBufferedReader(edgesPgdf, StandardCharsets.UTF_8)) {
            String line; String[] header = null;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                if (line.startsWith("@")) {
                    header = Arrays.stream(line.split("\\|")).map(String::trim).toArray(String[]::new);
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

                byte[] edgeKey = KeySchema.keyEdge(edgeId);
                byte[] edgeValue = EdgeBlob.encode(label, src, dst, props);

                batch.put(cfEdges, edgeKey, edgeValue);
                currentBatchBytes += edgeKey.length + edgeValue.length;

                byte[] idxLabelKey = KeySchema.idxKey("label","edge", label, edgeId);
                batch.put(cfIndex, idxLabelKey, EMPTY_VALUE);
                currentBatchBytes += idxLabelKey.length;

                byte[] idxSrcNodeKey = KeySchema.idxKey("label","srcnodes", label, src);
                batch.put(cfIndex, idxSrcNodeKey, EMPTY_VALUE);
                currentBatchBytes += idxSrcNodeKey.length;
                
                byte[] idxDstNodeKey = KeySchema.idxKey("label","dstnodes", label, dst);
                batch.put(cfIndex, idxDstNodeKey, EMPTY_VALUE);
                currentBatchBytes += idxDstNodeKey.length;
                
                byte[] idxBySrcKey = KeySchema.idxKey("edgesBySrc", src, edgeId);
                batch.put(cfIndex, idxBySrcKey, EMPTY_VALUE);
                currentBatchBytes += idxBySrcKey.length;

                byte[] idxByDstKey = KeySchema.idxKey("edgesByDst", dst, edgeId);
                batch.put(cfIndex, idxByDstKey, EMPTY_VALUE);
                currentBatchBytes += idxByDstKey.length;

                for (var e : props.entrySet()){
                    if (e.getValue()==null || e.getValue().isEmpty()) continue;
                    String normValue = KeySchema.norm(e.getValue());
                    byte[] indexKey = KeySchema.idxKey("propEdge", e.getKey(), normValue, edgeId);
                    
                    batch.put(cfIndex, indexKey, EMPTY_VALUE);
                    currentBatchBytes += indexKey.length;
                }

                // update schema
                metaStore.addEdgeConnection(label, nodeIdToLabelCache.get(src), nodeIdToLabelCache.get(dst));

                batchEdgeCount++;
                batchEdgeCountByLabel.merge(label, 1L, Long::sum);
                Set<String> propsForLabel = batchEdgeProps.computeIfAbsent(label, k -> new HashSet<>());
                propsForLabel.addAll(props.keySet());

                if (currentBatchBytes > MAX_BATCH_SIZE_BYTES) {
                    db.write(writeOptions, batch);
                    
                    metaStore.incEdgeCount(batchEdgeCount);
                    metaStore.addEdgeLabel(label);
                    batchEdgeCountByLabel.forEach(metaStore::incEdgeCountByLabel);
                    batchEdgeProps.forEach((lbl, propSet) -> 
                        propSet.forEach(p -> metaStore.addEdgeProp(lbl, p))
                    );

                    batch.close();
                    batch = new WriteBatch();
                    currentBatchBytes = 0;
                    batchEdgeCount = 0;
                    batchEdgeCountByLabel.clear();
                    batchEdgeProps.clear();
                }
            }

            if (batchEdgeCount > 0) {
                db.write(writeOptions, batch);
                
                metaStore.incEdgeCount(batchEdgeCount);
                batchEdgeCountByLabel.forEach(metaStore::incEdgeCountByLabel);
                batchEdgeCountByLabel.keySet().forEach(metaStore::addEdgeLabel); 
                batchEdgeProps.forEach((lbl, propSet) -> 
                    propSet.forEach(p -> metaStore.addEdgeProp(lbl, p))
                );
            }

        } finally {
            batch.close();
            writeOptions.close();
        }
    }

    @Override public void close() throws IOException {
        if (nodeIdToLabelCache != null){
            nodeIdToLabelCache.clear();
            nodeIdToLabelCache = null;
        }
     }
}