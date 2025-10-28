package com.gdblab.graphstorage;

import org.rocksdb.RocksDBException;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

class ExampleUsage {

    public static void main(String[] args) throws Exception {
        try (GraphStore store = GraphStore.open(Path.of("./graphdb"))) {
            store.ingestor().ingestNodes(Path.of("nodes_ok.pgdf"));
            store.ingestor().ingestEdges(Path.of("edges_ok.pgdf"));

            var q = store.queries();

            printSection("Graph Metadata");
            long n = q.getNodesQuantity();
            long e = q.getEdgesQuantity();
            Map<String, Long> eByLabel = q.getEdgesQuantityByLabel();
            System.out.println("Total Nodes     : " + n);
            System.out.println("Total Edges : " + e);
            System.out.println("Edges by label:");
            eByLabel.forEach((lbl, cnt) -> System.out.println("  - " + lbl + " : " + cnt));
            System.out.println();

            printSection("Structure and properties");
            Map<String, Set<String>> nodeStruct = q.getNodesStructure();
            Map<String, Set<String>> edgeStruct = q.getEdgesStructure();

            System.out.println("Nodes (label -> props):");
            nodeStruct.forEach((label, props) ->
                    System.out.println("  - " + label + " -> " + props));

            System.out.println("\nEdges (label -> props):");
            edgeStruct.forEach((label, props) ->
                    System.out.println("  - " + label + " -> " + props));
            System.out.println();

            printSection("Nodes iterator");
            List<GraphQueries.NodeEntry> allNodes = toList(q.getNodeIterator());
            for (var ne : allNodes) {
                var b = ne.blob();
                System.out.println(fmtNode(ne.id(), b.label, b.props));
            }
            System.out.println();

            printSection("Edges iterator");
            List<GraphQueries.EdgeEntry> allEdges = toList(q.getEdgeIterator());
            for (var ee : allEdges) {
                var b = ee.blob();
                System.out.println(fmtEdge(ee.id(), b.label, b.src, b.dst, b.props));
            }
            System.out.println();

            printSection("Edges by label iterator");
            for (String label : sorted(edgeStruct.keySet())) {
                System.out.println("Label: " + label);
                for (var ee : q.getEdgeIteratorByLabel(label)) {
                    var b = ee.blob();
                    System.out.println("  " + fmtEdge(ee.id(), b.label, b.src, b.dst, b.props));
                }
            }
            System.out.println();

            printSection("Neighbours (3 nodes)");
            List<String> someNodeIds = allNodes.stream().map(GraphQueries.NodeEntry::id).limit(3).toList();
            for (String nodeId : someNodeIds) {
                System.out.println("Node: " + nodeId);
                for (var ee : q.getNeighbours(nodeId)) {
                    var b = ee.blob();
                    System.out.println("  " + fmtEdge(ee.id(), b.label, b.src, b.dst, b.props));
                }
            }
            System.out.println();

            printSection("GET NODE / GET EDGE");
            if (!allNodes.isEmpty()) {
                var first = allNodes.get(0);
                var nb = q.getNode(first.id());
                System.out.println("getNode(\"" + first.id() + "\") => " + fmtNode(first.id(), nb.label, nb.props));
            }
            if (!allEdges.isEmpty()) {
                var firstE = allEdges.get(0);
                var eb = q.getEdge(firstE.id());
                System.out.println("getEdge(\"" + firstE.id() + "\") => " + fmtEdge(firstE.id(), eb.label, eb.src, eb.dst, eb.props));
            }
            System.out.println();

            printSection("Key=value search on nodes ");
            findAndRunNodePropEqualsDemo(q, allNodes);

            printSection("End");

        } catch (RocksDBException ex) {
            ex.printStackTrace();
        }
    }
    private static void printSection(String title) {
        String bar = "─".repeat(Math.max(10, title.length() + 2));
        System.out.println();
        System.out.println(bar);
        System.out.println(" " + title);
        System.out.println(bar);
    }

    private static String fmtNode(String id, String label, Map<String, String> props) {
        return "Node{id=" + id + ", label=" + label + ", props=" + propsToStr(props) + "}";
    }

    private static String fmtEdge(String id, String label, String src, String dst, Map<String, String> props) {
        return "Edge{id=" + id + ", label=" + label + ", src=" + src + " -> dst=" + dst + ", props=" + propsToStr(props) + "}";
    }

    private static String propsToStr(Map<String, String> props) {
        if (props == null || props.isEmpty()) return "{}";
        return props.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + "=" + quoteIfNeeded(e.getValue()))
                .collect(Collectors.joining(", ", "{", "}"));
    }

    private static String quoteIfNeeded(String s) {
        if (s == null) return "null";
        boolean needs = s.contains(" ") || s.contains("|") || s.contains(":");
        return needs ? "\"" + s + "\"" : s;
    }

    private static <T> List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList<>();
        for (T t : it) list.add(t);
        return list;
    }

    private static List<String> sorted(Collection<String> c) {
        ArrayList<String> l = new ArrayList<>(c);
        Collections.sort(l);
        return l;
    }

    private static void findAndRunNodePropEqualsDemo(GraphQueries q, List<GraphQueries.NodeEntry> allNodes) {
        for (var ne : allNodes) {
            for (var e : ne.blob().props.entrySet()) {
                String prop = e.getKey();
                String value = e.getValue();
                if (prop == null || prop.isBlank() || value == null || value.isBlank()) continue;

                System.out.println("Demo: forEachNodeByPropertyEquals(prop=\"" + prop + "\", value=\"" + value + "\")");
                List<String> hits = new ArrayList<>();
                q.forEachNodeByPropertyEquals(prop, value, hits::add);
                for (String id : hits) {
                    try {
                        var nb = q.getNode(id);
                        System.out.println("  " + fmtNode(id, nb.label, nb.props));
                    } catch (RocksDBException ex) {
                        System.out.println("  (error leyendo nodo " + id + "): " + ex.getMessage());
                    }
                }
                return; 
            }
        }
        System.out.println("Key value not found");
    }

    private static void findAndRunEdgePropEqualsDemo(GraphQueries q, List<GraphQueries.EdgeEntry> allEdges) {
        for (var ee : allEdges) {
            var props = ee.blob().props;
            if (props == null) continue;
            for (var e : props.entrySet()) {
                String prop = e.getKey();
                String value = e.getValue();
                if (prop == null || prop.isBlank() || value == null || value.isBlank()) continue;

                System.out.println("Demo: forEachEdgeByPropertyEquals(prop=\"" + prop + "\", value=\"" + value + "\")");
                List<String> edgeIds = new ArrayList<>();
                q.forEachEdgeByPropertyEquals(prop, value, edgeIds::add);
                for (String id : edgeIds) {
                    try {
                        var eb = q.getEdge(id);
                        System.out.println("  " + fmtEdge(id, eb.label, eb.src, eb.dst, eb.props));
                    } catch (RocksDBException ex) {
                        System.out.println("  (error leyendo arista " + id + "): " + ex.getMessage());
                    }
                }
                return; 
            }
        }
        System.out.println("Key value not found");
    }
}
