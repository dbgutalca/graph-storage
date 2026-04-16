package com.gdblab.graphstorage.engine;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

public class KeySchema {
    public static final byte SEP = 0;

    public static byte[] keyNode(String nodeId) { return ("node:" + nodeId).getBytes(StandardCharsets.UTF_8); }
    public static byte[] keyEdge(String edgeId) { return ("edge:" + edgeId).getBytes(StandardCharsets.UTF_8); }

    public static byte[] idxKey(String... parts) {
        if (parts.length == 0) return new byte[0];
        int totalLen = 0;
        for (int i = 0; i < parts.length; i++) {
            totalLen += parts[i].getBytes(StandardCharsets.UTF_8).length;
            if (i > 0) totalLen++; // SEP
        }
        byte[] out = new byte[totalLen];
        int offset = 0;
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) out[offset++] = SEP;
            byte[] b = parts[i].getBytes(StandardCharsets.UTF_8);
            System.arraycopy(b, 0, out, offset, b.length);
            offset += b.length;
        }
        return out;
    }
    public static byte[] idxPrefix(String... partsWithoutLast) { return idxKey(partsWithoutLast); }

    public static boolean startsWith(byte[] a, byte[] p) {
        if (a.length < p.length) return false;
        for (int i=0;i<p.length;i++) if (a[i]!=p[i]) return false;
        return true;
    }

    public static String suffixAfterPrefix(byte[] key, byte[] prefix) {
        int start = prefix.length;
        if (start < key.length && key[start] == SEP) start++;
        return new String(key, start, key.length - start, StandardCharsets.UTF_8);
    }

    public static String norm(String s) { return s==null? "" : s.toLowerCase(Locale.ROOT); }

    public static String makeEdgeId(String src, String label, String dst) {
        String s = src + "|" + label + "|" + dst;
        long x = 1125899906842597L; 
        for (int i=0;i<s.length();i++) x = (x * 1315423911L) ^ s.charAt(i);
        return Long.toUnsignedString(x);
    }
}
