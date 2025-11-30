package com.gdblab.graphstorage.storage;


import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import com.gdblab.graphstorage.storage.Utils.AutoCloseableIterable;
import com.gdblab.graphstorage.storage.GraphQueries.EdgeEntry;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

class IndexStore  {
    private final RocksDB db;
    private final ColumnFamilyHandle cfIndex;

    IndexStore(RocksDB db, ColumnFamilyHandle cfIndex) {
        this.db = db;
        this.cfIndex = cfIndex;
    }
  
    // Reading idxs by their prefix
    public void forEachEdgeIdByLabel(String label, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxPrefix("label","edge",label);
        scan(prefix, consumer);
    }

    public void forEachNodeByPropertyEquals(String propName, String propValue, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxPrefix("prop", propName, KeySchema.norm(propValue));
        scan(prefix, consumer);
    }

    public void forEachEdgeByPropertyEquals(String propName, String propValue, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxPrefix("propEdge", propName, KeySchema.norm(propValue));
        scan(prefix, consumer);
    }

    public void forEachEdgeIdBySrc(String srcNodeId, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxPrefix("edgesBySrc", srcNodeId);
        scan(prefix, consumer);
    }

    public void forEachEdgeIdByDst(String dstNodeId, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxPrefix("edgesByDst", dstNodeId);
        scan(prefix, consumer);
    }

    private void scan(byte[] prefix, Consumer<String> consumer){
        try (RocksIterator it = db.newIterator(cfIndex)) {
            it.seek(prefix);
            while (it.isValid() && KeySchema.startsWith(it.key(), prefix)) {
                consumer.accept(KeySchema.suffixAfterPrefix(it.key(), prefix));
                it.next();
            }
        }
    }

    // CHANGES for lazy iterables

    private AutoCloseableIterable<String> scanLazy(byte[] prefix){
        final RocksIterator it = db.newIterator(cfIndex);

        return new AutoCloseableIterable<String>() {
           private boolean closed = false;
           
           @Override
           public Iterator<String> iterator(){
            if (closed) {
                throw new IllegalStateException("Iterator has been closed");
            }

            it.seek(prefix);

            return new Iterator<String>(){
                private String nextVal = null;
                private boolean hasNextCalled = false;


                @Override
                public boolean hasNext(){
                    if (closed) return false;
                    if (hasNextCalled) return nextVal != null;

                    hasNextCalled = true;
                    nextVal = null;

                    if (it.isValid() && KeySchema.startsWith(it.key(), prefix)){
                        nextVal = KeySchema.suffixAfterPrefix(it.key(), prefix);
                        it.next();
                    }
                    return nextVal != null;
                }

                @Override
                public String next(){
                    if (!hasNext()){
                        throw new NoSuchElementException();
                    }
                    String current = nextVal;
                    hasNextCalled = false;
                    nextVal = null;
                    return current;
                }

            };

           }

           @Override
              public void close(){
                if (!closed){
                    it.close();
                    closed = true;
                }
              }
        };
    }

    public AutoCloseableIterable<String> getEdgeIdsByLabel(String label){
        return scanLazy(KeySchema.idxPrefix("label","edge",label));
    }
    public AutoCloseableIterable<String> getEdgesIdsBySrc(String srcNodeId){
        return scanLazy(KeySchema.idxPrefix("edgesBySrc", srcNodeId));
    }

    public AutoCloseableIterable<String> getEdgesIdsByDst(String dstNodeId){
        return scanLazy(KeySchema.idxPrefix("edgesByDst", dstNodeId));
    }
    public AutoCloseableIterable<String> getNodesByPropertyEquals(String propName, String propValue){
        return scanLazy(KeySchema.idxPrefix("prop", propName, KeySchema.norm(propValue)));
    }
    public AutoCloseableIterable<String> getEdgesByPropertyEquals(String propName, String propValue){
        return scanLazy(KeySchema.idxPrefix("propEdge", propName, KeySchema.norm(propValue)));
    }
    
    public AutoCloseableIterable<EdgeEntry> getEdgeEntriesByLabel(String label) {
    byte[] prefix = KeySchema.idxPrefix("label", "edge", label);
    final RocksIterator it = db.newIterator(cfIndex);

    return new AutoCloseableIterable<EdgeEntry>() {
        private boolean closed = false;

        @Override
        public Iterator<EdgeEntry> iterator() {
            if (closed) throw new IllegalStateException("Closed");
            it.seek(prefix);

            return new Iterator<EdgeEntry>() {
                private EdgeEntry nextVal = null;

                @Override
                public boolean hasNext() {
                    if (nextVal != null) return true;
                    if (!it.isValid()) return false;
                    if (!KeySchema.startsWith(it.key(), prefix)) return false;

                    String id = KeySchema.suffixAfterPrefix(it.key(), prefix);
                    
                    EdgeBlob blob = EdgeBlob.decode(it.value());
                    nextVal = new EdgeEntry(id, blob);
                    it.next();
                    return true;
                }

                @Override
                public EdgeEntry next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    EdgeEntry ret = nextVal;
                    nextVal = null;
                    return ret;
                }
            };
        }

        @Override
        public void close() {
            if (!closed) { it.close(); closed = true; }
        }
    };
}

    static final byte[] EMPTY_VALUE = new byte[0];

}