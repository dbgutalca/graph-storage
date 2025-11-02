package com.gdblab.graphstorage.storage;


import org.rocksdb.*;

import com.gdblab.graphstorage.storage.Utils.AutoCloseableIterable;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;


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


    /* 
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
    }*/

    // =================================================================
    public AutoCloseableIterable<GraphQueries.NodeEntry> scanAll() {
        final RocksIterator it = db.newIterator(cf);

        return new AutoCloseableIterable<>() {
            private boolean closed = false;

            @Override
            public Iterator<GraphQueries.NodeEntry> iterator() {
                if (closed) {
                    throw new IllegalStateException("Iterator has been closed");
                }
                
                it.seekToFirst(); 

                return new Iterator<>() {
                    private GraphQueries.NodeEntry nextVal = null;
                    private boolean hasNextCalled = false;

                    @Override
                    public boolean hasNext() {
                        if (closed) return false;
                        if (hasNextCalled) return nextVal != null;

                        hasNextCalled = true;
                        nextVal = null;

                        if (it.isValid()) {
                            byte[] k = it.key();
                            byte[] v = it.value();
                            
                            String id = new String(k, StandardCharsets.UTF_8).substring("node:".length());
                            nextVal = new GraphQueries.NodeEntry(id, NodeBlob.decode(v));
                            
                            it.next(); 
                        }
                        return nextVal != null; 
                    }

                    @Override
                    public GraphQueries.NodeEntry next() {
                        if (!hasNext()) { 
                            throw new NoSuchElementException();
                        }
                        GraphQueries.NodeEntry current = nextVal;
                        hasNextCalled = false;
                        nextVal = null;
                        return current;
                    }
                };
            }

            @Override
            public void close() {
                if (!closed) {
                    it.close();
                    closed = true;
                }
            }
        };
    }

}

