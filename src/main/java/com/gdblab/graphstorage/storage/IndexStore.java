package com.gdblab.graphstorage.storage;


import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import com.gdblab.graphstorage.storage.Utils.AutoCloseableIterable;

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

    // writing indexes 
    public void putNodePropEq(String propName, String propValue, String nodeId) throws RocksDBException {
        String norm = KeySchema.norm(propValue);
        byte[] k = KeySchema.idxKey("prop", propName, norm, nodeId);
        db.put(cfIndex, k, EMPTY_VALUE);
    }

    public void putEdgePropEq(String propName, String propValue, String edgeId) throws RocksDBException {
        String norm = KeySchema.norm(propValue);
        byte[] k = KeySchema.idxKey("propEdge", propName, norm, edgeId);
        db.put(cfIndex, k, EMPTY_VALUE);
    }

    public void putEdgeLabel(String label, String edgeId) throws RocksDBException {
        db.put(cfIndex, KeySchema.idxKey("label","edge", label, edgeId), EMPTY_VALUE);
    }

    public void putSrcNodeByLabel(String label, String srcNodeId) throws RocksDBException {
        db.put(cfIndex, KeySchema.idxKey("label","srcnodes", label, srcNodeId), EMPTY_VALUE);
    }

    public void putDstNodeByLabel(String label, String dstNodeId) throws RocksDBException {
        db.put(cfIndex, KeySchema.idxKey("label","dstnodes", label, dstNodeId), EMPTY_VALUE);
    }

    public void putEdgeBySrc(String srcNodeId, String EdgeId) throws RocksDBException{
        db.put(cfIndex, KeySchema.idxKey("edgesBySrc", srcNodeId, EdgeId), EMPTY_VALUE);
    }

    public void putEdgeByDst(String dstNodeId,  String EdgeId) throws RocksDBException{
        db.put(cfIndex, KeySchema.idxKey("edgesByDst", dstNodeId, EdgeId), EMPTY_VALUE);
    }

    // Reading idxs by their prefix
    public void forEachEdgeIdByLabel(String label, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxPrefix("label","edge",label);
        scan(prefix, consumer);
    }

    public void forEachSourceNodeByLabel(String label, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxPrefix("label","srcnodes",label);
        scan(prefix, consumer);
    }

    public void forEachDestinationNodeByLabel(String label, Consumer<String> consumer){
        byte[] prefix = KeySchema.idxPrefix("label","dstnodes",label);
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
    

    static final byte[] EMPTY_VALUE = new byte[0];

}