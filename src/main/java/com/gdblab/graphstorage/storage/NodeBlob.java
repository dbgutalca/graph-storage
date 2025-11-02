package com.gdblab.graphstorage.storage;


import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public final class NodeBlob {
    public final String label;
    public final Map<String,String> props;

    public NodeBlob(String label, Map<String, String> props) {
        this.label = label;
        this.props = props;
    }

    /* [u16 labLen][lab][u16 propCount]{ [u16 kLen][k][u32 vLen][v] } */
    public static byte[] encode(String label, Map<String,String> props){
        byte[] lb = label.getBytes(StandardCharsets.UTF_8);
        int size = 2 + lb.length + 2;
        for (var e: props.entrySet()){
            size += 2 + e.getKey().getBytes(StandardCharsets.UTF_8).length
                    + 4 + (e.getValue()==null?0:e.getValue().getBytes(StandardCharsets.UTF_8).length);
        }
        ByteBuffer bb = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        bb.putShort((short)lb.length).put(lb);
        bb.putShort((short)props.size());
        for (var e: props.entrySet()){
            byte[] k = e.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] v = (e.getValue()==null? new byte[0] : e.getValue().getBytes(StandardCharsets.UTF_8));
            bb.putShort((short)k.length).put(k);
            bb.putInt(v.length).put(v);
        }
        return bb.array();
    }

    public static NodeBlob decode(byte[] b){
        ByteBuffer bb = ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN);
        int ll = bb.getShort() & 0xFFFF; byte[] lb = new byte[ll]; bb.get(lb);
        String label = new String(lb, StandardCharsets.UTF_8);
        int pc = bb.getShort() & 0xFFFF;
        Map<String,String> props = new LinkedHashMap<>(pc);
        for (int i=0;i<pc;i++){
            int kl = bb.getShort() & 0xFFFF; byte[] kb = new byte[kl]; bb.get(kb);
            String k = new String(kb, StandardCharsets.UTF_8);
            int vl = bb.getInt(); byte[] vb = new byte[vl]; bb.get(vb);
            String v = new String(vb, StandardCharsets.UTF_8);
            props.put(k, v);
        }
        return new NodeBlob(label, props);
    }
}
