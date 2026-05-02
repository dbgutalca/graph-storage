package com.gdblab.graphstorage.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.NoSuchElementException;


import org.rocksdb.RocksDBException;

import com.gdblab.graphstorage.storage.Utils.AutoCloseableIterable;
import com.gdblab.graphstorage.storage.Utils.GraphStorageException;
import com.gdblab.graphstorage.engine.NodeStore;
import com.gdblab.graphstorage.engine.EdgeStore;
import com.gdblab.graphstorage.engine.IndexStore;
import com.gdblab.graphstorage.engine.MetaStore;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/** API for querying */
public class GraphQueries implements Closeable {
    private final IndexStore index;
    private final EdgeStore edges;
    private final NodeStore nodes;
    private final MetaStore meta;

    GraphQueries(NodeStore nodes, EdgeStore edges, IndexStore index, MetaStore meta) {
        this.nodes = nodes;
        this.edges = edges;
        this.index = index;
        this.meta = meta;
    }

    // Basics
    public NodeBlob getNode(String nodeId) throws RocksDBException {
        return nodes.get(nodeId);
    }

    public Map<String, NodeBlob> multiGetNodes(List<String> nodeIds) throws RocksDBException {
        return nodes.multiGet(nodeIds);
    }

    public EdgeBlob getEdge(String edgeId) throws RocksDBException {
        return edges.get(edgeId);
    }


    // Structure
    public long getNodesQuantity() {
        return meta.getNodeCount();
    }

    public long getEdgesQuantity() {
        return meta.getEdgeCount();
    }

    public Map<String, Long> getEdgesQuantityByLabel() {
        return meta.getEdgeCountsByLabel();
    }

    public Map<String, Set<String>> getNodesStructure() {
        return meta.getNodeSchema();
    }

    public Map<String, Set<String>> getEdgesStructure() {
        return meta.getEdgeSchema();
    }
    public String getJsonMetadata() {
        return meta.toJsonString(); 
    }

    public void forEachEdgeIdByLabel(String label, Consumer<String> consumer) {
        index.forEachEdgeIdByLabel(label, consumer);
    }

    public void forEachNodeByPropertyEquals(String propName, String propValue, Consumer<String> consumer) {
        index.forEachNodeByPropertyEquals(propName, propValue, consumer);
    }

    public void forEachEdgeByPropertyEquals(String propName, String propValue, Consumer<String> consumer) {
        index.forEachEdgeByPropertyEquals(propName, propValue, consumer);
    }

    @Override
    public void close() throws IOException {
    }

    public record NodeEntry(String id, NodeBlob blob) {
    }

    public record EdgeEntry(String id, EdgeBlob blob) {
    }

    private <T, R> AutoCloseableIterable<R> mapLazy(
            AutoCloseableIterable<T> source,
            Function<T, R> mapper) {

        return new AutoCloseableIterable<R>() {
            @Override
            public Iterator<R> iterator() {
                Iterator<T> originalIterator = source.iterator();
                return new Iterator<R>() {
                    @Override
                    public boolean hasNext() {
                        return originalIterator.hasNext();
                    }

                    @Override
                    public R next() {
                        T original = originalIterator.next();
                        return mapper.apply(original);
                    }
                };
            }

            @Override
            public void close() {
                source.close();
            }
        };
    }

    // Iterators

    public AutoCloseableIterable<NodeEntry> getNodeIterator() {
        AutoCloseableIterable<NodeStore.NodeEntry> engineIt = nodes.scanAll();
        return mapLazy(engineIt, e -> new NodeEntry(e.id(), e.blob()));
    }

    public AutoCloseableIterable<EdgeEntry> getEdgeIterator() {
        AutoCloseableIterable<EdgeStore.EdgeEntry> engineIt = edges.scanAll();
        return mapLazy(engineIt, e -> new EdgeEntry(e.id(), e.blob()));
    }

    public AutoCloseableIterable<EdgeEntry> getEdgeIteratorByLabel(String label) {
        AutoCloseableIterable<IndexStore.RawEdgeEntry> source = index.getRawEdgeEntriesByLabel(label);
        
        return new AutoCloseableIterable<EdgeEntry>() {
            @Override
            public Iterator<EdgeEntry> iterator() {
                Iterator<IndexStore.RawEdgeEntry> sourceIt = source.iterator();
                
                return new Iterator<EdgeEntry>() {
                    private final List<EdgeEntry> currentBatch = new ArrayList<>();
                    private int batchIdx = 0;

                    @Override
                    public boolean hasNext() {
                        if (batchIdx < currentBatch.size()) return true;
                        if (!sourceIt.hasNext()) return false;
                        
                        currentBatch.clear();
                        batchIdx = 0;
                        byte[] raw = sourceIt.next().rawData();
                        ByteBuffer bb = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
                        int count = bb.getInt();
                        for (int i=0; i<count; i++) {
                            int len = bb.getInt();
                            byte[] mega = new byte[len];
                            bb.get(mega);
                            ByteBuffer megaBB = ByteBuffer.wrap(mega).order(ByteOrder.BIG_ENDIAN);
                            int edgeLen = megaBB.getInt();
                            byte[] edgeBytes = new byte[edgeLen];
                            megaBB.get(edgeBytes);
                            
                            EdgeBlob.FastEdge fast = EdgeBlob.decodeFast(edgeBytes);
                            currentBatch.add(new EdgeEntry(fast.id(), EdgeBlob.decode(edgeBytes)));
                        }
                        return !currentBatch.isEmpty();
                    }

                    @Override
                    public EdgeEntry next() {
                        if (!hasNext()) throw new NoSuchElementException();
                        return currentBatch.get(batchIdx++);
                    }
                };
            }

            @Override
            public void close() {
                source.close();
            }
        };
    }

    public record RawEdgeEntry(byte[] rawData) {}

    public AutoCloseableIterable<RawEdgeEntry> getRawEdgeIteratorByLabel(String label) {
        AutoCloseableIterable<IndexStore.RawEdgeEntry> engineIt = index.getRawEdgeEntriesByLabel(label);
        return mapLazy(engineIt, e -> new RawEdgeEntry(e.rawData()));
    }

    public AutoCloseableIterable<EdgeEntry> getNeighbours(String nodeId) {

        AutoCloseableIterable<String> idsSrc = index.getEdgesIdsBySrc(nodeId);
        AutoCloseableIterable<String> idsDst = index.getEdgesIdsByDst(nodeId);

        AutoCloseableIterable<String> allIds = chainLazy(idsSrc, idsDst);
        return mapLazy(allIds, id -> {
            try {
                EdgeBlob blob = edges.get(id);
                if (blob == null)
                    return null;
                return new EdgeEntry(id, blob);
            } catch (RocksDBException e) {
                throw new GraphStorageException("Failed to get edge with ID: " + id, e);
            }
        });
    }

    public AutoCloseableIterable<NodeEntry> getNodesByPropertyEquals(String propName, String propValue) {
        AutoCloseableIterable<String> ids = index.getNodesByPropertyEquals(propName, propValue);

        return mapLazy(ids, id -> {
            try {
                NodeBlob blob = nodes.get(id);
                if (blob == null)
                    return null;
                return new NodeEntry(id, blob);
            } catch (RocksDBException e) {
                throw new GraphStorageException("Failed to get node with ID: " + id, e);
            }
        });
    }

    public AutoCloseableIterable<EdgeEntry> getEdgesByPropertyEquals(String propName, String propValue) {
        AutoCloseableIterable<String> ids = index.getEdgesByPropertyEquals(propName, propValue);

        return mapLazy(ids, id -> {
            try {
                EdgeBlob blob = edges.get(id);
                if (blob == null)
                    return null;
                return new EdgeEntry(id, blob);
            } catch (RocksDBException e) {
                throw new GraphStorageException("Failed to get edge with ID: " + id, e);
            }
        });
    }

    private <T> AutoCloseableIterable<T> chainLazy(
            AutoCloseableIterable<T> first,
            AutoCloseableIterable<T> second) {

        return new AutoCloseableIterable<T>() {
            @Override
            public Iterator<T> iterator() {
                Iterator<T> firstIt = first.iterator();
                Iterator<T> secondIt = second.iterator();

                return new Iterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return firstIt.hasNext() || secondIt.hasNext();
                    }

                    @Override
                    public T next() {
                        if (firstIt.hasNext()) {
                            return firstIt.next();
                        }
                        if (secondIt.hasNext()) {
                            return secondIt.next();
                        }
                        throw new NoSuchElementException();
                    }
                };
            }

            @Override
            public void close() {
                try {
                    first.close();
                } finally {
                    second.close();
                }
            }
        };
    }

}
