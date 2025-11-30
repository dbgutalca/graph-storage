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
    private static final long MAX_BATCH_SIZE_BYTES = 16_777_216L; // 16MB
    private static final byte[] EMPTY_VALUE = new byte[0];

    private final ArrayList<String> reuseCols = new ArrayList<>(100);

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
        writeOptions.setDisableWAL(true);
        writeOptions.setSync(false);
        WriteBatch batch = new WriteBatch(); 
        long currentBatchBytes = 0; 

        long batchNodeCount = 0;
        Set<String> batchNodeLabels = new HashSet<>();
        Map<String, Set<String>> batchNodeProps = new HashMap<>();

        try (BufferedReader br = Files.newBufferedReader(nodesPgdf, StandardCharsets.UTF_8)) {
            String line; 
            String[] header = null;
            
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                
                if (line.startsWith("@")) {
                    header = Arrays.stream(line.split("\\|")).map(String::trim).toArray(String[]::new);
                    continue;
                }
                if (header == null) continue;

                fastSplit(line, reuseCols);
                
                String nodeId = "";
                String label = "";
                Map<String, String> props = new LinkedHashMap<>();

                for (int i = 0; i < header.length; i++) {
                    String val = (i < reuseCols.size()) ? reuseCols.get(i) : "";
                    String key = header[i];

                    if (key.equals("@id")) {
                        nodeId = val.trim();
                    } else if (key.equals("@label")) {
                        label = val.trim();
                    } else if (!key.startsWith("@")) {
                        props.put(key, val);
                    }
                }

                if (nodeId.isEmpty() || label.isEmpty()) continue;

                nodeIdToLabelCache.put(nodeId, label);

                byte[] nodeKey = KeySchema.keyNode(nodeId);
                byte[] nodeValue = NodeBlob.encode(label, props);

                batch.put(cfNodes, nodeKey, nodeValue);
                currentBatchBytes += nodeKey.length + nodeValue.length;

                for (var e : props.entrySet()){
                    if (e.getValue()==null || e.getValue().isEmpty()) continue;
                    String normValue = KeySchema.norm(e.getValue());
                    byte[] indexKey = KeySchema.idxKey("prop", e.getKey(), normValue, nodeId);
                    batch.put(cfIndex, indexKey, EMPTY_VALUE);
                    currentBatchBytes += indexKey.length;
                }
                
                batchNodeCount++;
                batchNodeLabels.add(label);
                Set<String> propsForLabel = batchNodeProps.computeIfAbsent(label, k -> new HashSet<>());
                propsForLabel.addAll(props.keySet());

                if (currentBatchBytes > MAX_BATCH_SIZE_BYTES) {
                    db.write(writeOptions, batch);
                    metaStore.incNodeCount(batchNodeCount);
                    batchNodeLabels.forEach(metaStore::addNodeLabel);
                    batchNodeProps.forEach((lbl, propSet) -> 
                        propSet.forEach(p -> metaStore.addNodeProp(lbl, p))
                    );
                    batch.close(); 
                    batch = new WriteBatch();
                    currentBatchBytes = 0;
                    batchNodeCount = 0;
                    batchNodeLabels.clear();
                    batchNodeProps.clear();
                }
            } 
            
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
        writeOptions.setDisableWAL(true);
        writeOptions.setSync(false);
        WriteBatch batch = new WriteBatch();
        long currentBatchBytes = 0;

        long batchEdgeCount = 0;
        Map<String, Long> batchEdgeCountByLabel = new HashMap<>();
        Map<String, Set<String>> batchEdgeProps = new HashMap<>();

        try (BufferedReader br = Files.newBufferedReader(edgesPgdf, StandardCharsets.UTF_8)) {
            String line; 
            String[] header = null;
            
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                
                if (line.startsWith("@")) {
                    header = Arrays.stream(line.split("\\|")).map(String::trim).toArray(String[]::new);
                    continue;
                }
                if (header == null) continue;

                fastSplit(line, reuseCols);

                String edgeId = "";
                String label = "";
                String dir = "T";
                String src = "";
                String dst = "";
                Map<String, String> props = new LinkedHashMap<>();

                for (int i = 0; i < header.length; i++) {
                    String val = (i < reuseCols.size()) ? reuseCols.get(i) : "";
                    String key = header[i];

                    if (key.equals("@id")) edgeId = val.trim();
                    else if (key.equals("@label")) label = val.trim();
                    else if (key.equals("@out")) src = val.trim();
                    else if (key.equals("@in")) dst = val.trim();
                    else if (key.equals("@dir")) dir = val.trim();
                    else if (!key.startsWith("@")) {
                        props.put(key, val);
                    }
                }

                if (label.isEmpty() || src.isEmpty() || dst.isEmpty()) continue;
                if (!"T".equalsIgnoreCase(dir)) continue; 
                
                if (edgeId.isEmpty()) edgeId = KeySchema.makeEdgeId(src, label, dst);

                byte[] edgeKey = KeySchema.keyEdge(edgeId);
                byte[] edgeValue = EdgeBlob.encode(label, src, dst, props);

                batch.put(cfEdges, edgeKey, edgeValue);
                currentBatchBytes += edgeKey.length + edgeValue.length;

                byte[] idxLabelKey = KeySchema.idxKey("label","edge", label, edgeId);
                batch.put(cfIndex, idxLabelKey, edgeValue); 
                currentBatchBytes += idxLabelKey.length;

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

                String srcLabel = nodeIdToLabelCache.get(src);
                String dstLabel = nodeIdToLabelCache.get(dst);
                metaStore.addEdgeConnection(label, srcLabel, dstLabel);

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

    private void fastSplit(String text, ArrayList<String> buffer) {
        buffer.clear();
        int start = 0;
        int end;
        while ((end = text.indexOf('|', start)) != -1) {
            buffer.add(text.substring(start, end));
            start = end + 1;
        }
        buffer.add(text.substring(start));
    }
}