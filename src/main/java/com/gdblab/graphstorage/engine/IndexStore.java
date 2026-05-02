package com.gdblab.graphstorage.engine;


import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import com.gdblab.graphstorage.storage.EdgeBlob;
import com.gdblab.graphstorage.storage.Utils.AutoCloseableIterable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
        byte[] prefix = (label + ":").getBytes();
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
        return scanLazy(cfIdxLabel, (label + ":").getBytes());
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

    public static record RawEdgeEntry(byte[] rawData) {}

    public AutoCloseableIterable<RawEdgeEntry> getRawEdgeEntriesByLabel(String label) {
        byte[] prefix = (label + ":").getBytes();
        byte[] upperBound = prefix.clone();
        upperBound[upperBound.length - 1]++; 

        final ReadOptions readOptions = new ReadOptions()
            .setIterateUpperBound(new Slice(upperBound))
            .setReadaheadSize(4 * 1024 * 1024); 
            
        final RocksIterator it = db.newIterator(cfIdxLabel, readOptions);

        return new AutoCloseableIterable<RawEdgeEntry>() {
            private boolean closed = false;

            @Override
            public Iterator<RawEdgeEntry> iterator() {
                if (closed) throw new IllegalStateException("Closed");
                it.seek(prefix);

                return new Iterator<RawEdgeEntry>() {
                    @Override
                    public boolean hasNext() {
                        if (closed) return false;
                        return it.isValid();
                    }

                    @Override
                    public RawEdgeEntry next() {
                        if (!hasNext()) throw new NoSuchElementException();
                        RawEdgeEntry entry = new RawEdgeEntry(it.value()); // JNI Call per FAT BATCH (1 per 1000 records)
                        it.next(); // JNI Call per FAT BATCH
                        return entry;
                    }
                };
            }

            @Override
            public void close() {
                if (!closed) { it.close(); readOptions.close(); closed = true; }
            }
        };
    }
    
    static final byte[] EMPTY_VALUE = new byte[0];

}
