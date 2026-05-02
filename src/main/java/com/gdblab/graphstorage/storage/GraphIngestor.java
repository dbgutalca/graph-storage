package com.gdblab.graphstorage.storage;

import org.rocksdb.*;
import com.gdblab.graphstorage.engine.MetaStore;
import com.gdblab.graphstorage.engine.KeySchema;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class GraphIngestor implements Closeable {
    
    private final RocksDB db;
    private final ColumnFamilyHandle cfNodes;
    private final ColumnFamilyHandle cfEdges;
    private final ColumnFamilyHandle cfIdxNodeProp;
    private final ColumnFamilyHandle cfIdxEdgeProp;
    private final ColumnFamilyHandle cfIdxEdgeSrc;
    private final ColumnFamilyHandle cfIdxEdgeDst;
    private final ColumnFamilyHandle cfIdxLabel;
    private final MetaStore metaStore;

    private Map<String, byte[]> nodeIdToBlobCache;

    private static final long MAX_BATCH_SIZE_BYTES = 16_777_216L; 
    private static final byte[] EMPTY_VALUE = new byte[0];

    private final ArrayList<String> reuseCols = new ArrayList<>(100);

    // Grouping for Fat-Indexing
    private final Map<String, List<byte[]>> labelToPendingMegaBlobs = new HashMap<>();

    GraphIngestor(RocksDB db, 
                  ColumnFamilyHandle cfNodes, 
                  ColumnFamilyHandle cfEdges, 
                  ColumnFamilyHandle cfIdxNodeProp,
                  ColumnFamilyHandle cfIdxEdgeProp,
                  ColumnFamilyHandle cfIdxEdgeSrc,
                  ColumnFamilyHandle cfIdxEdgeDst,
                  ColumnFamilyHandle cfIdxLabel,
                  MetaStore metaStore) {
        this.db = db;
        this.cfNodes = cfNodes;
        this.cfEdges = cfEdges;
        this.cfIdxNodeProp = cfIdxNodeProp;
        this.cfIdxEdgeProp = cfIdxEdgeProp;
        this.cfIdxEdgeSrc = cfIdxEdgeSrc;
        this.cfIdxEdgeDst = cfIdxEdgeDst;
        this.cfIdxLabel = cfIdxLabel;
        this.metaStore = metaStore;
        this.nodeIdToBlobCache = new HashMap<>();
    }

    public void ingestNodes(Path nodesPgdf) throws IOException, RocksDBException {
        WriteOptions writeOptions = new WriteOptions().setDisableWAL(true).setSync(false);
        WriteBatch batch = new WriteBatch(); 
        long currentBatchBytes = 0; 
        long batchNodeCount = 0;
        Map<String, Long> batchNodeCountByLabel = new HashMap<>();
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
                fastSplit(line, reuseCols);
                String nodeId = ""; String label = "";
                Map<String, String> props = new LinkedHashMap<>();
                for (int i = 0; i < header.length; i++) {
                    String val = (i < reuseCols.size()) ? reuseCols.get(i).trim() : "";
                    String key = header[i];
                    if (key.equals("@id")) nodeId = val;
                    else if (key.equals("@label")) label = val;
                    else if (!key.startsWith("@")) props.put(key, val);
                }
                if (nodeId.isEmpty() || label.isEmpty()) continue;
                byte[] nodeValue = NodeBlob.encode(label, props);
                nodeIdToBlobCache.put(nodeId, nodeValue);
                byte[] nodeKey = KeySchema.keyNode(nodeId);
                batch.put(cfNodes, nodeKey, nodeValue);
                currentBatchBytes += nodeKey.length + nodeValue.length;
                for (var e : props.entrySet()){
                    if (e.getValue()==null || e.getValue().isEmpty()) continue;
                    byte[] indexKey = KeySchema.idxKey(e.getKey(), KeySchema.norm(e.getValue()), nodeId);
                    batch.put(cfIdxNodeProp, indexKey, EMPTY_VALUE);
                    currentBatchBytes += indexKey.length;
                }
                batchNodeCount++;
                batchNodeCountByLabel.merge(label, 1L, Long::sum);
                batchNodeLabels.add(label);
                batchNodeProps.computeIfAbsent(label, k -> new HashSet<>()).addAll(props.keySet());
                if (currentBatchBytes > MAX_BATCH_SIZE_BYTES) {
                    flushNodesBatch(writeOptions, batch, batchNodeCount, batchNodeCountByLabel, batchNodeLabels, batchNodeProps);
                    batch.close(); batch = new WriteBatch(); currentBatchBytes = 0;
                    batchNodeCount = 0; batchNodeCountByLabel.clear(); batchNodeLabels.clear(); batchNodeProps.clear();
                }
            } 
            if (batchNodeCount > 0) flushNodesBatch(writeOptions, batch, batchNodeCount, batchNodeCountByLabel, batchNodeLabels, batchNodeProps);
        } finally { batch.close(); writeOptions.close(); }
    }

    private void flushNodesBatch(WriteOptions wo, WriteBatch batch, long count, Map<String, Long> countsByLabel, Set<String> labels, Map<String, Set<String>> props) throws RocksDBException {
        db.write(wo, batch);
        metaStore.incNodeCount(count);
        countsByLabel.forEach(metaStore::incNodeCountByLabel);
        labels.forEach(metaStore::addNodeLabel);
        props.forEach((lbl, propSet) -> propSet.forEach(p -> metaStore.addNodeProp(lbl, p)));
    }

    public void ingestEdges(Path edgesPgdf) throws IOException, RocksDBException {
        WriteOptions writeOptions = new WriteOptions().setDisableWAL(true).setSync(false);
        WriteBatch batch = new WriteBatch();
        long currentBatchBytes = 0;
        long batchEdgeCount = 0;
        Map<String, Long> batchEdgeCountByLabel = new HashMap<>();
        Map<String, Map<MetaStore.EdgeConnection, Long>> batchConnCounts = new HashMap<>();
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
                fastSplit(line, reuseCols);
                String edgeId = ""; String label = ""; String dir = "T"; String src = ""; String dst = "";
                Map<String, String> props = new LinkedHashMap<>();
                for (int i = 0; i < header.length; i++) {
                    String val = (i < reuseCols.size()) ? reuseCols.get(i).trim() : "";
                    String key = header[i];
                    if (key.equals("@id")) edgeId = val;
                    else if (key.equals("@label")) label = val;
                    else if (key.equals("@out")) src = val;
                    else if (key.equals("@in")) dst = val;
                    else if (key.equals("@dir")) dir = val;
                    else if (!key.startsWith("@")) props.put(key, val);
                }
                if (label.isEmpty() || src.isEmpty() || dst.isEmpty() || !"T".equalsIgnoreCase(dir)) continue;
                if (edgeId.isEmpty()) edgeId = KeySchema.makeEdgeId(src, label, dst);

                byte[] edgeKey = KeySchema.keyEdge(edgeId);
                byte[] edgeValue = EdgeBlob.encode(edgeId, label, src, dst, props);
                batch.put(cfEdges, edgeKey, edgeValue);
                currentBatchBytes += edgeKey.length + edgeValue.length;

                // fat indexin group blobs in batches of 1000 per label
                byte[] megaBlob = EdgeBlob.encodeMega(edgeValue, nodeIdToBlobCache.get(src), nodeIdToBlobCache.get(dst));
                List<byte[]> pending = labelToPendingMegaBlobs.computeIfAbsent(label, k -> new ArrayList<>(1000));
                pending.add(megaBlob);
                if (pending.size() >= 1000) {
                    byte[] fatBatchKey = (label + ":" + UUID.randomUUID().toString()).getBytes(StandardCharsets.UTF_8);
                    batch.put(cfIdxLabel, fatBatchKey, EdgeBlob.encodeFatBatch(pending));
                    pending.clear();
                }


                batch.put(cfIdxEdgeSrc, KeySchema.idxKey(src, edgeId), EMPTY_VALUE);
                batch.put(cfIdxEdgeDst, KeySchema.idxKey(dst, edgeId), EMPTY_VALUE);
                for (var e : props.entrySet()){
                    if (e.getValue()==null || e.getValue().isEmpty()) continue;
                    batch.put(cfIdxEdgeProp, KeySchema.idxKey(e.getKey(), KeySchema.norm(e.getValue()), edgeId), EMPTY_VALUE);
                }

                // Metadata stats
                byte[] sB = nodeIdToBlobCache.get(src); byte[] dB = nodeIdToBlobCache.get(dst);
                if (sB != null && dB != null) {
                    batchConnCounts.computeIfAbsent(label, k -> new HashMap<>())
                        .merge(new MetaStore.EdgeConnection(NodeBlob.decode(sB).label, NodeBlob.decode(dB).label), 1L, Long::sum);
                }
                batchEdgeCount++;
                batchEdgeCountByLabel.merge(label, 1L, Long::sum);
                batchEdgeProps.computeIfAbsent(label, k -> new HashSet<>()).addAll(props.keySet());

                if (currentBatchBytes > MAX_BATCH_SIZE_BYTES) {
                    flushEdgesBatch(writeOptions, batch, batchEdgeCount, batchEdgeCountByLabel, batchConnCounts, batchEdgeProps);
                    batch.close(); batch = new WriteBatch(); currentBatchBytes = 0;
                    batchEdgeCount = 0; batchEdgeCountByLabel.clear(); batchConnCounts.clear(); batchEdgeProps.clear();
                }
            }
            // Flush remaining Fat Batches
            for (var entry : labelToPendingMegaBlobs.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    byte[] fatBatchKey = (entry.getKey() + ":" + UUID.randomUUID().toString()).getBytes(StandardCharsets.UTF_8);
                    batch.put(cfIdxLabel, fatBatchKey, EdgeBlob.encodeFatBatch(entry.getValue()));
                }
            }
            if (batchEdgeCount > 0) flushEdgesBatch(writeOptions, batch, batchEdgeCount, batchEdgeCountByLabel, batchConnCounts, batchEdgeProps);
        } finally { batch.close(); writeOptions.close(); labelToPendingMegaBlobs.clear(); }
    }

    private void flushEdgesBatch(WriteOptions wo, WriteBatch batch, long count, Map<String, Long> countsByLabel, Map<String, Map<MetaStore.EdgeConnection, Long>> connCounts, Map<String, Set<String>> props) throws RocksDBException {
        db.write(wo, batch);
        metaStore.incEdgeCount(count);
        countsByLabel.forEach(metaStore::incEdgeCountByLabel);
        countsByLabel.keySet().forEach(metaStore::addEdgeLabel);
        connCounts.forEach((edgeLabel, innerMap) -> innerMap.forEach((conn, quantity) -> metaStore.incEdgeConnection(edgeLabel, conn.srcLabel(), conn.dstLabel(), quantity)));
        props.forEach((lbl, propSet) -> propSet.forEach(p -> metaStore.addEdgeProp(lbl, p)));
    }

    @Override public void close() throws IOException { if (nodeIdToBlobCache != null) nodeIdToBlobCache.clear(); }
    private void fastSplit(String text, ArrayList<String> buffer) {
        buffer.clear(); int start = 0; int end;
        while ((end = text.indexOf('|', start)) != -1) { buffer.add(text.substring(start, end)); start = end + 1; }
        buffer.add(text.substring(start));
    }
}
