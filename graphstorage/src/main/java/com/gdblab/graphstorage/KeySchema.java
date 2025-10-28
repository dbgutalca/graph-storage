package com.gdblab.graphstorage;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

class KeySchema {
    static final byte SEP = 0;

    static byte[] keyNode(String nodeId) { return ("node:" + nodeId).getBytes(StandardCharsets.UTF_8); }
    static byte[] keyEdge(String edgeId) { return ("edge:" + edgeId).getBytes(StandardCharsets.UTF_8); }

    static byte[] idxKey(String... parts) {
        int len = 3; // "idx"
        for (String p : parts) len += 1 + p.getBytes(StandardCharsets.UTF_8).length; // SEP + bytes
        byte[] out = new byte[len];
        int i = 0;
        byte[] idx = "idx".getBytes(StandardCharsets.UTF_8);
        System.arraycopy(idx, 0, out, i, idx.length); i += idx.length;
        for (String p : parts) {
            out[i++] = SEP;
            byte[] b = p.getBytes(StandardCharsets.UTF_8);
            System.arraycopy(b, 0, out, i, b.length);
            i += b.length;
        }
        return out;
    }
    static byte[] idxPrefix(String... partsWithoutLast) { return idxKey(partsWithoutLast); }

    static boolean startsWith(byte[] a, byte[] p) {
        if (a.length < p.length) return false;
        for (int i=0;i<p.length;i++) if (a[i]!=p[i]) return false;
        return true;
    }

    static String suffixAfterPrefix(byte[] key, byte[] prefix) {
        int start = prefix.length;
        if (start < key.length && key[start] == SEP) start++;
        return new String(key, start, key.length - start, StandardCharsets.UTF_8);
    }

    static String norm(String s) { return s==null? "" : s.toLowerCase(Locale.ROOT); }

    static String makeEdgeId(String src, String label, String dst) {
        String s = src + "|" + label + "|" + dst;
        long x = 1125899906842597L; 
        for (int i=0;i<s.length();i++) x = (x * 1315423911L) ^ s.charAt(i);
        return Long.toUnsignedString(x);
    }
}

