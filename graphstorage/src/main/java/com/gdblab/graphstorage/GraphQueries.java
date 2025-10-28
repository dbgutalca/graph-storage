package com.gdblab.graphstorage;


import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

import org.rocksdb.RocksDBException;

import java.util.*;

/** API for querying  */
public class GraphQueries implements Closeable {
    private final IndexStore index;
    private final EdgeStore edges;
    private final NodeStore nodes;
    private final MetaStore meta;

    GraphQueries(NodeStore nodes, EdgeStore edges, IndexStore index, MetaStore meta){
        this.nodes = nodes;
        this.edges = edges;
        this.index = index;
        this.meta = meta;
    }

    // Basics
    public NodeBlob getNode(String nodeId) throws RocksDBException{
        return nodes.get(nodeId);
    }
    public EdgeBlob getEdge(String edgeId) throws RocksDBException{
        return edges.get(edgeId);
    }

    // Iterators
    public Iterable<NodeEntry> getNodeIterator(){return nodes.scanAll();}
    public Iterable<EdgeEntry> getEdgeIterator(){return edges.scanAll();}

    public Iterable<EdgeEntry> getEdgeIteratorByLabel(String label){
        List<String> ids = new ArrayList<>();
        index.forEachEdgeIdByLabel(label,ids::add);
        return () -> ids.stream().map(id -> {
            try { return new EdgeEntry(id, edges.get(id));}
            catch (RocksDBException e) { throw new RuntimeException(e); }
        }).iterator();
    }

    // Neighbors
    public Iterable<EdgeEntry> getNeighbours(String nodeId){
        List<String> ids = new ArrayList<>();
        index.forEachEdgeIdBySrc(nodeId, ids::add);
        index.forEachEdgeIdByDst(nodeId, ids::add);
        return () -> ids.stream().map(id -> {
            try { return new EdgeEntry(id, edges.get(id)); }
            catch (RocksDBException e) { throw new RuntimeException(e); }
        }).iterator();
    }

    // Structure
    public long getNodesQuantity() throws RocksDBException {
        return meta.getNodeCount();
    }
    public long getEdgesQuantity() throws RocksDBException {
        return meta.getEdgeCount();
    }


    public Map<String, Long> getEdgesQuantityByLabel() throws RocksDBException {
        return meta.getEdgeCountsByLabel();
    }

    public Map<String, Set<String>> getNodesStructure() throws RocksDBException {
        return meta.getNodeSchema();
    }
    public Map<String, Set<String>> getEdgesStructure() throws RocksDBException {
        return meta.getEdgeSchema();
    }


    public void forEachEdgeIdByLabel(String label, Consumer<String> consumer){ index.forEachEdgeIdByLabel(label, consumer); }

    public void forEachSourceNodeByLabel(String label, Consumer<String> consumer){ index.forEachSourceNodeByLabel(label, consumer); }

    public void forEachDestinationNodeByLabel(String label, Consumer<String> consumer){ index.forEachDestinationNodeByLabel(label, consumer); }

    public void forEachNodeByPropertyEquals(String propName, String propValue, Consumer<String> consumer){ index.forEachNodeByPropertyEquals(propName, propValue, consumer); }

    public void forEachEdgeByPropertyEquals(String propName, String propValue, Consumer<String> consumer){ index.forEachEdgeByPropertyEquals(propName, propValue, consumer); }

    @Override public void close() throws IOException { }

    public record NodeEntry(String id, NodeBlob blob) {}
    public record EdgeEntry(String id, EdgeBlob blob) {}

}

