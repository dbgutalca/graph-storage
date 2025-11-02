package com.gdblab.graphstorage.storage;


import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
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

    /* [u16 labLen][lab][u16 srcLen][src][u16 dstLen][dst][u16 propCount]{ [u16 kLen][k][u32 vLen][v] } */
    public static byte[] encode(String label, String src, String dst, Map<String,String> props){
        byte[] lb = label.getBytes(StandardCharsets.UTF_8);
        byte[] sb = src.getBytes(StandardCharsets.UTF_8);
        byte[] db = dst.getBytes(StandardCharsets.UTF_8);
        int size = 2+lb.length + 2+sb.length + 2+db.length + 2;
        for (var e: props.entrySet()){
            size += 2 + e.getKey().getBytes(StandardCharsets.UTF_8).length
                    + 4 + (e.getValue()==null?0:e.getValue().getBytes(StandardCharsets.UTF_8).length);
        }
        ByteBuffer bb = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
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

    public static EdgeBlob decode(byte[] b){
        ByteBuffer bb = ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN);
        int ll=bb.getShort()&0xFFFF; byte[] lb=new byte[ll]; bb.get(lb);
        int sl=bb.getShort()&0xFFFF; byte[] sb=new byte[sl]; bb.get(sb);
        int dl=bb.getShort()&0xFFFF; byte[] db=new byte[dl]; bb.get(db);
        String label = new String(lb, StandardCharsets.UTF_8);
        String src = new String(sb, StandardCharsets.UTF_8);
        String dst = new String(db, StandardCharsets.UTF_8);
        int pc = bb.getShort() & 0xFFFF;
        Map<String,String> props = new LinkedHashMap<>(pc);
        for (int i=0;i<pc;i++){
            int kl = bb.getShort() & 0xFFFF; byte[] kb = new byte[kl]; bb.get(kb);
            String k = new String(kb, StandardCharsets.UTF_8);
            int vl = bb.getInt(); byte[] vb = new byte[vl]; bb.get(vb);
            String v = new String(vb, StandardCharsets.UTF_8);
            props.put(k, v);
        }
        return new EdgeBlob(label, src, dst, props);
    }
}

