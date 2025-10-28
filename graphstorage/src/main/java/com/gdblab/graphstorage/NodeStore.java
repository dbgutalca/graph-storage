package com.gdblab.graphstorage;


import org.rocksdb.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;


class NodeStore  {
    private final RocksDB db;
    private final ColumnFamilyHandle cf;

    NodeStore(RocksDB db, ColumnFamilyHandle cf) { this.db = db; this.cf = cf; }

    public void put(String nodeId, String label, Map<String,String> props) throws RocksDBException {
        db.put(cf, KeySchema.keyNode(nodeId), NodeBlob.encode(label, props));
    }

    public NodeBlob get(String nodeId) throws RocksDBException {
        byte[] v = db.get(cf, KeySchema.keyNode(nodeId));
        return v==null? null : NodeBlob.decode(v);
    }

    public Iterable<GraphQueries.NodeEntry> scanAll(){
        return () -> new Iterator<>(){
            final RocksIterator it = db.newIterator(cf);{
                it.seekToFirst();
            }
            @Override public boolean hasNext() { return it.isValid(); }
            @Override public GraphQueries.NodeEntry next() {
                byte[] k = it.key(); byte[] v = it.value(); it.next();
                String id = new String(k, StandardCharsets.UTF_8).substring("node:".length());
                return new GraphQueries.NodeEntry(id, NodeBlob.decode(v));
            }
        };
    }

}

