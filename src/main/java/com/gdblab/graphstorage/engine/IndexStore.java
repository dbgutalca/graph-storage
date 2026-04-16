package com.gdblab.graphstorage.engine;


import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import com.gdblab.graphstorage.storage.EdgeBlob;
import com.gdblab.graphstorage.storage.Utils.AutoCloseableIterable;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class IndexStore  {
    private final RocksDB db;
    private final ColumnFamilyHandle cfIdxNodeProp;
    private final ColumnFamilyHandle cfIdxEdgeProp;
    private final ColumnFamilyHandle cfIdxEdgeSrc;
    private final ColumnFamilyHandle cfIdxEdgeDst;
    private final ColumnFamilyHandle cfIdxLabel;

    public IndexStore(RocksDB db, 
               ColumnFamilyHandle cfIdxNodeProp,
               ColumnFamilyHandle cfIdxEdgeProp,
               ColumnFamilyHandle cfIdxEdgeSrc,
               ColumnFamilyHandle cfIdxEdgeDst,
               ColumnFamilyHandle cfIdxLabel) {
        this.db = db;
        this.cfIdxNodeProp = cfIdxNodeProp;
        this.cfIdxEdgeProp = cfIdxEdgeProp;
        this.cfIdxEdgeSrc = cfIdxEdgeSrc;
        this.cfIdxEdgeDst = cfIdxEdgeDst;
        this.cfIdxLabel = cfIdxLabel;
    }
  
    public void forEachEdgeIdByLabel(String label, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxKey(label, "");
        scan(cfIdxLabel, prefix, consumer);
    }

    public void forEachNodeByPropertyEquals(String propName, String propValue, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxKey(propName, KeySchema.norm(propValue), "");
        scan(cfIdxNodeProp, prefix, consumer);
    }

    public void forEachEdgeByPropertyEquals(String propName, String propValue, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxKey(propName, KeySchema.norm(propValue), "");
        scan(cfIdxEdgeProp, prefix, consumer);
    }

    public void forEachEdgeIdBySrc(String srcNodeId, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxKey(srcNodeId, "");
        scan(cfIdxEdgeSrc, prefix, consumer);
    }

    public void forEachEdgeIdByDst(String dstNodeId, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxKey(dstNodeId, "");
        scan(cfIdxEdgeDst, prefix, consumer);
    }

    private void scan(ColumnFamilyHandle cf, byte[] prefix, Consumer<String> consumer){
        try (RocksIterator it = db.newIterator(cf)) {
            it.seek(prefix);
            while (it.isValid() && KeySchema.startsWith(it.key(), prefix)) {
                consumer.accept(KeySchema.suffixAfterPrefix(it.key(), prefix));
                it.next();
            }
        }
    }


    private AutoCloseableIterable<String> scanLazy(ColumnFamilyHandle cf, byte[] prefix){
        final RocksIterator it = db.newIterator(cf);

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
        return scanLazy(cfIdxLabel, KeySchema.idxKey(label, ""));
    }
    public AutoCloseableIterable<String> getEdgesIdsBySrc(String srcNodeId){
        return scanLazy(cfIdxEdgeSrc, KeySchema.idxKey(srcNodeId, ""));
    }

    public AutoCloseableIterable<String> getEdgesIdsByDst(String dstNodeId){
        return scanLazy(cfIdxEdgeDst, KeySchema.idxKey(dstNodeId, ""));
    }
    public AutoCloseableIterable<String> getNodesByPropertyEquals(String propName, String propValue){
        return scanLazy(cfIdxNodeProp, KeySchema.idxKey(propName, KeySchema.norm(propValue), ""));
    }
    public AutoCloseableIterable<String> getEdgesByPropertyEquals(String propName, String propValue){
        return scanLazy(cfIdxEdgeProp, KeySchema.idxKey(propName, KeySchema.norm(propValue), ""));
    }

    // Optimized scan for edge entries by label (Covering Index)
    public AutoCloseableIterable<EdgeStore.EdgeEntry> getEdgeEntriesByLabel(String label) {
        byte[] prefix = KeySchema.idxKey(label, "");
        final RocksIterator it = db.newIterator(cfIdxLabel);

        return new AutoCloseableIterable<EdgeStore.EdgeEntry>() {
            private boolean closed = false;

            @Override
            public Iterator<EdgeStore.EdgeEntry> iterator() {
                if (closed) throw new IllegalStateException("Closed");
                it.seek(prefix);

                return new Iterator<EdgeStore.EdgeEntry>() {
                    private EdgeStore.EdgeEntry nextVal = null;
                    private boolean hasNextCalled = false;

                    @Override
                    public boolean hasNext() {
                        if (closed) return false;
                        if (hasNextCalled) return nextVal != null;

                        hasNextCalled = true;
                        nextVal = null;

                        if (it.isValid() && KeySchema.startsWith(it.key(), prefix)) {
                            String id = KeySchema.suffixAfterPrefix(it.key(), prefix);
                            EdgeBlob blob = EdgeBlob.decode(it.value());
                            nextVal = new EdgeStore.EdgeEntry(id, blob);
                            it.next();
                        }
                        return nextVal != null;
                    }

                    @Override
                    public EdgeStore.EdgeEntry next() {
                        if (!hasNext()) throw new NoSuchElementException();
                        EdgeStore.EdgeEntry current = nextVal;
                        hasNextCalled = false;
                        nextVal = null;
                        return current;
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
