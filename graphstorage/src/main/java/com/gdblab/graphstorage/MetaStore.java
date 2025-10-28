package com.gdblab.graphstorage;

import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

class MetaStore {
    private final RocksDB db;
    private final ColumnFamilyHandle cf;

    MetaStore(RocksDB db, ColumnFamilyHandle cf) { this.db = db; this.cf = cf; }

    private static byte[] kNodeCount(){
        return "meta|counts|nodes".getBytes(StandardCharsets.UTF_8);
    }
    private static byte[] kEdgeCount(){
        return "meta|counts|edges".getBytes(StandardCharsets.UTF_8);
    }
    private static byte[] kEdgeLabelCount(String label){
        return ("meta|counts|edgeLabel|"+label).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] kNodeLabel(String label){ return ("schema|nodeLabels|"+label).getBytes(StandardCharsets.UTF_8); }

    private static byte[] kNodeProp(String label, String prop){ return ("schema|nodeProps|"+label+"|"+prop).getBytes(StandardCharsets.UTF_8); }

    private static byte[] kEdgeLabel(String label){ return ("schema|edgeLabels|"+label).getBytes(StandardCharsets.UTF_8); }

    private static byte[] kEdgeProp(String label, String prop){ return ("schema|edgeProps|"+label+"|"+prop).getBytes(StandardCharsets.UTF_8); }

    private static byte[] encLong(long v){return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(v).array();}

    private static long decLong(byte[] b){ if (b==null) return 0L; return ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN).getLong(); }

    public void incNodeCount(long delta) throws RocksDBException {
        long cur = decLong(db.get(cf, kNodeCount()));
        db.put(cf, kNodeCount(), encLong(cur + delta));
    }

    public void incEdgeCount(long delta) throws RocksDBException {
        long cur = decLong(db.get(cf, kEdgeCount()));
        db.put(cf, kEdgeCount(), encLong(cur + delta));
    }

    public void incEdgeCountByLabel(String label, long delta) throws RocksDBException {
        byte[] k = kEdgeLabelCount(label);
        long cur = decLong(db.get(cf, k));
        db.put(cf, k, encLong(cur + delta));
    }

    public long getNodeCount() throws RocksDBException { return decLong(db.get(cf, kNodeCount())); }

    public long getEdgeCount() throws RocksDBException { return decLong(db.get(cf, kEdgeCount())); }

  public Map<String, Long> getEdgeCountsByLabel() throws RocksDBException {
        Map<String, Long> out = new LinkedHashMap<>();
        try (RocksIterator it = db.newIterator(cf)){
            byte[] prefix = "meta|counts|edgeLabel|".getBytes(StandardCharsets.UTF_8);
            it.seek(prefix);
            while (it.isValid()){
                byte[] k = it.key();
                if (!startsWith(k, prefix)) break;
                String full = new String(k, StandardCharsets.UTF_8);
                String label = full.substring("meta|counts|edgeLabel|".length());
                out.put(label, decLong(it.value()));
                it.next();
            }
        }
        return out;
    }

      public void addNodeLabel(String label) throws RocksDBException { db.put(cf, kNodeLabel(label), EMPTY); }
    public void addNodeProp(String label, String prop) throws RocksDBException { db.put(cf, kNodeProp(label, prop), EMPTY); }
    public void addEdgeLabel(String label) throws RocksDBException { db.put(cf, kEdgeLabel(label), EMPTY); }
    public void addEdgeProp(String label, String prop) throws RocksDBException { db.put(cf, kEdgeProp(label, prop), EMPTY); }

    public Map<String, Set<String>> getNodeSchema(){
        return readSchema("schema|nodeLabels|", "schema|nodeProps|");
    }
    public Map<String, Set<String>> getEdgeSchema(){
        return readSchema("schema|edgeLabels|", "schema|edgeProps|");
    }

    private Map<String, Set<String>> readSchema(String labelPrefixStr, String propPrefixStr){
        byte[] labelPrefix = labelPrefixStr.getBytes(StandardCharsets.UTF_8);
        byte[] propPrefix  = propPrefixStr.getBytes(StandardCharsets.UTF_8);
        Map<String, Set<String>> out = new LinkedHashMap<>();
        try (RocksIterator it = db.newIterator(cf)){
            // labels
            it.seek(labelPrefix);
            while (it.isValid() && startsWith(it.key(), labelPrefix)){
                String label = new String(it.key(), StandardCharsets.UTF_8).substring(labelPrefixStr.length());
                out.computeIfAbsent(label, k -> new LinkedHashSet<>());
                it.next();
            }
            // props
            it.seek(propPrefix);
            while (it.isValid() && startsWith(it.key(), propPrefix)){
                String tail = new String(it.key(), StandardCharsets.UTF_8).substring(propPrefixStr.length());
                int p = tail.indexOf('|'); if (p <= 0) { it.next(); continue; }
                String label = tail.substring(0, p);
                String prop  = tail.substring(p+1);
                out.computeIfAbsent(label, k -> new LinkedHashSet<>()).add(prop);
                it.next();
            }
        }
        return out;
    }

    private static boolean startsWith(byte[] a, byte[] p){
        if (a.length < p.length) return false; for (int i=0;i<p.length;i++) if (a[i]!=p[i]) return false; return true;
    }

    private static final byte[] EMPTY = new byte[0];


}