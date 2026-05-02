package com.gdblab.graphstorage.engine;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.gdblab.graphstorage.storage.EdgeBlob;
import com.gdblab.graphstorage.storage.Utils.AutoCloseableIterable;

import java.util.Iterator;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;

public class EdgeStore {
    private final RocksDB db;
    private final ColumnFamilyHandle cf;

    public record EdgeEntry(String id, EdgeBlob blob) {}

    public EdgeStore(RocksDB db, ColumnFamilyHandle cf) { this.db = db; this.cf = cf; }

    public void put(String edgeId, String label, String src, String dst, Map<String,String> props) throws RocksDBException {
        db.put(cf, KeySchema.keyEdge(edgeId), EdgeBlob.encode(edgeId, label, src, dst, props));
    }


    public EdgeBlob get(String edgeId) throws RocksDBException {
        byte[] v = db.get(cf, KeySchema.keyEdge(edgeId));
        return v==null? null : EdgeBlob.decode(v);
    }

    public AutoCloseableIterable<EdgeEntry> scanAll() {
        final RocksIterator it = db.newIterator(cf);
        
        return new AutoCloseableIterable<>() {
            private boolean closed = false;

            @Override
            public Iterator<EdgeEntry> iterator() {
                if (closed) {
                    throw new IllegalStateException("Iterator has been closed");
                }
                
                it.seekToFirst(); 

                return new Iterator<>() {
                    private EdgeEntry nextVal = null;
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
                            
                            String id = new String(k, StandardCharsets.UTF_8).substring("edge:".length());
                            nextVal = new EdgeEntry(id, EdgeBlob.decode(v));
                            
                            it.next(); 
                        }
                        return nextVal != null;
                    }

                    @Override
                    public EdgeEntry next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        EdgeEntry current = nextVal;
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
