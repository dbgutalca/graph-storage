package com.gdblab.graphstorage.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class EdgeBlob {
    public final String label;
    public final String src;
    public final String dst;
    public final Map<String,String> props;

    public EdgeBlob(String label, String src, String dst, Map<String,String> props) {
        this.label = label;
        this.src = src;
        this.dst = dst;
        this.props = props;
    }

    public static record FastEdge(String id, String src, String dst) {}

    public static byte[] encode(String id, String label, String src, String dst, Map<String,String> props){
        byte[] idb = id.getBytes(StandardCharsets.UTF_8);
        byte[] lb = label.getBytes(StandardCharsets.UTF_8);
        byte[] sb = src.getBytes(StandardCharsets.UTF_8);
        byte[] db = dst.getBytes(StandardCharsets.UTF_8);
        int size = 2+idb.length + 2+lb.length + 2+sb.length + 2+db.length + 2;
        for (var e: props.entrySet()){
            size += 2 + e.getKey().getBytes(StandardCharsets.UTF_8).length
                    + 4 + (e.getValue()==null?0:e.getValue().getBytes(StandardCharsets.UTF_8).length);
        }
        ByteBuffer bb = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        bb.putShort((short)idb.length).put(idb);
        bb.putShort((short)lb.length).put(lb);
        bb.putShort((short)sb.length).put(sb);
        bb.putShort((short)db.length).put(db);
        bb.putShort((short)props.size());
        for (var e: props.entrySet()){
            byte[] k = e.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] v = (e.getValue()==null? new byte[0] : e.getValue().getBytes(StandardCharsets.UTF_8));
            bb.putShort((short)k.length).put(k);
            bb.putInt(v.length).put(v);
        }
        return bb.array();
    }

    public static byte[] encodeLean(String edgeId, String src, String dst) {
        byte[] eb = edgeId.getBytes(StandardCharsets.UTF_8);
        byte[] sb = src.getBytes(StandardCharsets.UTF_8);
        byte[] db = dst.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(2 + eb.length + 2 + sb.length + 2 + db.length).order(ByteOrder.BIG_ENDIAN);
        bb.putShort((short)eb.length).put(eb);
        bb.putShort((short)sb.length).put(sb);
        bb.putShort((short)db.length).put(db);
        return bb.array();
    }

    public static byte[] encodeMega(byte[] edgeB, byte[] srcB, byte[] dstB) {
        int size = 4 + edgeB.length + 4 + (srcB == null ? 0 : srcB.length) + 4 + (dstB == null ? 0 : dstB.length);
        ByteBuffer bb = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(edgeB.length).put(edgeB);
        bb.putInt(srcB == null ? 0 : srcB.length);
        if (srcB != null) bb.put(srcB);
        bb.putInt(dstB == null ? 0 : dstB.length);
        if (dstB != null) bb.put(dstB);
        return bb.array();
    }

    public static byte[] encodeFatBatch(List<byte[]> megaBlobs) {
        int totalSize = 4; // count
        for (byte[] b : megaBlobs) totalSize += 4 + b.length;
        ByteBuffer bb = ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(megaBlobs.size());
        for (byte[] b : megaBlobs) {
            bb.putInt(b.length).put(b);
        }
        return bb.array();
    }

    public static FastEdge decodeFast(byte[] b) {
        return decodeFast(ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN));
    }

    public static FastEdge decodeFast(ByteBuffer bb) {
        int idl = bb.getShort() & 0xFFFF;
        byte[] idb = new byte[idl];
        bb.get(idb);
        String id = new String(idb, StandardCharsets.UTF_8);
        int ll = bb.getShort() & 0xFFFF;
        bb.position(bb.position() + ll); 
        int sl = bb.getShort() & 0xFFFF;
        byte[] sb = new byte[sl];
        bb.get(sb);
        String src = new String(sb, StandardCharsets.UTF_8);
        int dl = bb.getShort() & 0xFFFF;
        byte[] db = new byte[dl];
        bb.get(db);
        String dst = new String(db, StandardCharsets.UTF_8);
        return new FastEdge(id, src, dst);
    }

    public static EdgeBlob decode(byte[] b){
        return decode(ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN));
    }

    public static EdgeBlob decode(ByteBuffer bb){
        int idl = bb.getShort() & 0xFFFF;
        bb.position(bb.position() + idl); 
        int ll = bb.getShort() & 0xFFFF;
        byte[] lb = new byte[ll];
        bb.get(lb);
        String label = new String(lb, StandardCharsets.UTF_8);
        int sl = bb.getShort() & 0xFFFF;
        byte[] sb = new byte[sl];
        bb.get(sb);
        String src = new String(sb, StandardCharsets.UTF_8);
        int dl = bb.getShort() & 0xFFFF;
        byte[] db = new byte[dl];
        bb.get(db);
        String dst = new String(db, StandardCharsets.UTF_8);
        int pc = bb.getShort() & 0xFFFF;
        Map<String,String> props = new LinkedHashMap<>(pc);
        for (int i=0; i<pc; i++){
            int kl = bb.getShort() & 0xFFFF;
            byte[] kb = new byte[kl];
            bb.get(kb);
            String k = new String(kb, StandardCharsets.UTF_8);
            int vl = bb.getInt();
            byte[] vb = new byte[vl];
            bb.get(vb);
            String v = new String(vb, StandardCharsets.UTF_8);
            props.put(k, v);
        }
        return new EdgeBlob(label, src, dst, props);
    }
}
