package com.gdblab.graphstorage;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import java.util.Iterator;
import java.nio.charset.StandardCharsets;
import java.util.Map;

class EdgeStore {
    private final RocksDB db;
    private final ColumnFamilyHandle cf;

    EdgeStore(RocksDB db, ColumnFamilyHandle cf) { this.db = db; this.cf = cf; }

    public void put(String edgeId, String label, String src, String dst, Map<String,String> props) throws RocksDBException {
        db.put(cf, KeySchema.keyEdge(edgeId), EdgeBlob.encode(label, src, dst, props));
    }

    public EdgeBlob get(String edgeId) throws RocksDBException {
        byte[] v = db.get(cf, KeySchema.keyEdge(edgeId));
        return v==null? null : EdgeBlob.decode(v);
    }

    public Iterable<GraphQueries.EdgeEntry> scanAll(){
        return () -> new Iterator <>(){
            final RocksIterator it = db.newIterator(cf);
            {it.seekToFirst();}
            @Override public boolean hasNext() { return it.isValid(); }
            @Override public GraphQueries.EdgeEntry next() {
                byte[] k = it.key(); byte[] v = it.value(); it.next();
                String id = new String(k, StandardCharsets.UTF_8).substring("edge:".length());
                return new GraphQueries.EdgeEntry(id, EdgeBlob.decode(v));
            }
        };
    }

}

