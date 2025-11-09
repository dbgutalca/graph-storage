package com.gdblab.graphstorage;
import java.io.IOException;

import org.rocksdb.RocksDBException;

import com.gdblab.graphstorage.storage.*;
import com.gdblab.graphstorage.storage.Utils.GraphStorageException;
import com.gdblab.graphstorage.storage.Utils.AutoCloseableIterable; 
import com.gdblab.graphstorage.storage.GraphQueries;


import java.nio.file.Path;
import java.util.Map;
import java.util.Set;


public class GraphStorage implements AutoCloseable {

    private final GraphStore store;

    private GraphStorage(GraphStore store){
        this.store = store;
    }

    public static GraphStorage open(Path dbPath) throws GraphStorageException{
        try{
            GraphStore store = GraphStore.open(dbPath);
            return new GraphStorage(store);
        } catch (RocksDBException | IOException e){
            throw new GraphStorageException("Failed to open GraphStorage at " + dbPath, e);
        }
    }

    @Override
    public void close() {
        store.close();
    }

    //  Insert API
    public void insertNodesByFile(String filepath) throws GraphStorageException {
        try {
            store.ingestor().ingestNodes(Path.of(filepath));
        } catch (IOException | RocksDBException e) {
            throw new GraphStorageException("Failed to ingest nodes from file: " + filepath, e);
        }
    }

    public void insertEdgesByFile(String filepath) throws GraphStorageException {
        try {
            store.ingestor().ingestEdges(Path.of(filepath));
        } catch (IOException | RocksDBException e) {
            throw new GraphStorageException("Failed to ingest edges from file: " + filepath, e);
        }
    }

    //  Unique entity queries

    public NodeBlob getNode(String nodeId) throws GraphStorageException {
        try {
            return store.queries().getNode(nodeId);
        } catch (RocksDBException e) {
            throw new GraphStorageException("Failed to get node with ID: " + nodeId, e);
        }
    }

    public EdgeBlob getEdge(String edgeId) throws GraphStorageException {
        try {
            return store.queries().getEdge(edgeId);
        } catch (RocksDBException e) {
            throw new GraphStorageException("Failed to get edge with ID: " + edgeId, e);
        }
    }

    // Lazy iterators queries

    /**
     * Returns a lazy iterator over ALL nodes.
     * <p>
     * <b>Important!</b> this iterator MUST be closed. Always use it
     * with a try-with-resources block.
     * <pre>
     * {@code
     * try (var nodes = db.getNodeIterator()) {
     * for (var node : nodes) {
     * // ... do something with node.id() and node.blob()
     * }
     * }
     * }
     * </pre>
     * @return An auto-closeable iterator of NodeEntry.
     */
    public AutoCloseableIterable<GraphQueries.NodeEntry> getNodeIterator() {
        return store.queries().getNodeIterator();
    }

    /**
     * Returns a lazy iterator over ALL edges.
     * <p>
     * <b>Important!</b> this iterator MUST be closed. Always use it
     * with a try-with-resources block.
     *
     * @return An auto-closeable iterator of EdgeEntry.
     */
    public AutoCloseableIterable<GraphQueries.EdgeEntry> getEdgeIterator() {
        return store.queries().getEdgeIterator();
    }

    /**
     * Returns a lazy iterator over edges with a specific label.
     * <p>
     * <b>Important!</b> this iterator MUST be closed. Always use it
     * with a try-with-resources block.
     *
     * @param label The label to filter edges (e.g., "Knows")
     * @return An auto-closeable iterator of EdgeEntry.
     */
    public AutoCloseableIterable<GraphQueries.EdgeEntry> getEdgeIteratorByLabel(String label) {
        return store.queries().getEdgeIteratorByLabel(label);
    }

    /**
     * Returns a lazy iterator over the neighboring edges (outgoing and incoming) of a node.
     * <p>
     * <b>Important!</b> this iterator MUST be closed. Always use it
     * with a try-with-resources block.
     *
     * @param nodeId The ID of the central node.
     * @return An auto-closeable iterator of EdgeEntry.
     */
    public AutoCloseableIterable<GraphQueries.EdgeEntry> getNeighbours(String nodeId) {
        return store.queries().getNeighbours(nodeId);
    }

    /**
     * Returns a lazy iterator over nodes that have a property with a specific value.
     * <p>
     * <b>Important!</b> this iterator MUST be closed. Always use it
     * with a try-with-resources block.
     *
     * @param propName The name of the property (e.g., "name").
     * @param propValue The exact value to search for (e.g., "Juan").
     * @return An auto-closeable iterator of NodeEntry.
     */
    public AutoCloseableIterable<GraphQueries.NodeEntry> getNodesByPropertyEquals(String propName, String propValue) {
        return store.queries().getNodesByPropertyEquals(propName, propValue);
    }

    /**
     * Returns a lazy iterator over edges that have a property with a specific value.
     * <p>
     * <b>Important!</b> this iterator MUST be closed. Always use it
     * with a try-with-resources block.
     *
     * @param propName The name of the property (e.g., "weight").
     * @param propValue The exact value to search for (e.g., "5").
     * @return An auto-closeable iterator of EdgeEntry.
     */
    public AutoCloseableIterable<GraphQueries.EdgeEntry> getEdgesByPropertyEquals(String propName, String propValue) {
        return store.queries().getEdgesByPropertyEquals(propName, propValue);
    }


    //  Metadata Queries 

    public long getNodesQuantity() throws GraphStorageException {
        return store.queries().getNodesQuantity();
    }

    public long getEdgesQuantity() throws GraphStorageException {
        return store.queries().getEdgesQuantity();
    }

    public Map<String, Long> getEdgesQuantityByLabel() throws GraphStorageException {
        return store.queries().getEdgesQuantityByLabel();
    }

    public Map<String, Set<String>> getNodesStructure () throws GraphStorageException {
         return store.queries().getNodesStructure();
    }

    public Map<String, Set<String>> getEdgesStructure () throws GraphStorageException {
        return store.queries().getEdgesStructure();
    }
}