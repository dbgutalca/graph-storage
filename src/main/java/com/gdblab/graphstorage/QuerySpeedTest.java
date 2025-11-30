package com.gdblab.graphstorage;

import com.gdblab.graphstorage.storage.GraphQueries;
import com.gdblab.graphstorage.storage.NodeBlob;
import com.gdblab.graphstorage.storage.EdgeBlob;
import com.gdblab.graphstorage.storage.Utils.AutoCloseableIterable;
import com.gdblab.graphstorage.storage.Utils.GraphStorageException;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class QuerySpeedTest {

    // Configuración del Test
    private static final int ITERATIONS = 5; // Bajamos a 5 para ser más rápidos
    private static final int SAMPLE_SIZE = 100; // Muestreo más ligero
    private static final Path DB_PATH = Path.of("./db_sf3"); 

    public static void main(String[] args) {
        System.out.println("=================================================");
        System.out.println("   GRAPH STORAGE - QUERY SPEED BENCHMARK (FAST)");
        System.out.println("   Database: " + DB_PATH);
        System.out.println("   Iterations per test: " + ITERATIONS);
        System.out.println("=================================================\n");

        try (GraphStorage db = GraphStorage.open(DB_PATH)) {

            // --- FASE 1: PREPARACIÓN (Warm-up & Sampling) ---
            System.out.println(">> Analizando datos de muestra...");
            
            List<String> sampleNodeIds = new ArrayList<>();
            List<String> sampleEdgeIds = new ArrayList<>();
            String targetLabel = null;
            String targetPropKey = null;
            String targetPropVal = null;
            String highDegreeNodeId = null;

            try (var nodes = db.getNodeIterator()) {
                for (var node : nodes) {
                    if (sampleNodeIds.size() < SAMPLE_SIZE) sampleNodeIds.add(node.id());
                    if (targetPropKey == null && !node.blob().props.isEmpty()) {
                        var entry = node.blob().props.entrySet().iterator().next();
                        targetPropKey = entry.getKey();
                        targetPropVal = entry.getValue();
                    }
                    if (sampleNodeIds.size() >= SAMPLE_SIZE) break; 
                }
            }

            try (var edges = db.getEdgeIterator()) {
                int count = 0;
                for (var edge : edges) {
                    if (sampleEdgeIds.size() < SAMPLE_SIZE) sampleEdgeIds.add(edge.id());
                    if (targetLabel == null) targetLabel = edge.blob().label;
                    if (highDegreeNodeId == null) highDegreeNodeId = edge.blob().src;
                    if (count++ > SAMPLE_SIZE) break;
                }
            }

            System.out.println("   [Datos Recolectados]");
            System.out.println("   - Label objetivo: " + targetLabel);
            System.out.println("   - Propiedad objetivo: " + targetPropKey + "=" + targetPropVal);
            System.out.println("-------------------------------------------------\n");


            // --- FASE 2: EJECUCIÓN DE PRUEBAS ---

            // 1. Get Node (Random Access)
            // Hacemos 100 accesos aleatorios por iteración
            benchmark("1. Random Node Access (100 ops)", () -> {
                for (int i=0; i<100; i++) {
                    // Usamos modulo para ciclar si SAMPLE_SIZE < 100
                    String id = sampleNodeIds.get(i % sampleNodeIds.size());
                    db.getNode(id);
                }
            });

            // 2. Get Edge (Random Access)
            benchmark("2. Random Edge Access (100 ops)", () -> {
                for (int i=0; i<100; i++) {
                    String id = sampleEdgeIds.get(i % sampleEdgeIds.size());
                    db.getEdge(id);
                }
            });

            // 3. Scan All Nodes (Solo 1 iteración para no esperar años)
            System.out.print(String.format("%-50s", "3. Scan ALL Nodes (Iterator) [1 Run]"));
            long start = System.nanoTime();
            long nodeCount = 0;
            try (var it = db.getNodeIterator()) {
                for (var n : it) nodeCount++;
            }
            long end = System.nanoTime();
            System.out.println(String.format(" | Time: %8.3f ms | Items: %d", (end-start)/1_000_000.0, nodeCount));


            // 4. Scan All Edges (Solo 1 iteración)
            System.out.print(String.format("%-50s", "4. Scan ALL Edges (Iterator) [1 Run]"));
            start = System.nanoTime();
            long edgeCount = 0;
            try (var it = db.getEdgeIterator()) {
                for (var e : it) edgeCount++;
            }
            end = System.nanoTime();
            System.out.println(String.format(" | Time: %8.3f ms | Items: %d", (end-start)/1_000_000.0, edgeCount));


            // 5. Scan By Label (Covering Index Test)
            if (targetLabel != null) {
                final String lbl = targetLabel;
                benchmark("5. Scan Edges by Label '" + lbl + "'", () -> {
                    long count = 0;
                    try (var it = db.getEdgeIteratorByLabel(lbl)) {
                        for (var e : it) {
                            if (e.blob() != null) count++; 
                        }
                    }
                    return count;
                });
            }

            // 6. Get Neighbours
            if (highDegreeNodeId != null) {
                final String nodeId = highDegreeNodeId;
                benchmark("6. Get Neighbours of '" + nodeId + "'", () -> {
                    long count = 0;
                    try (var it = db.getNeighbours(nodeId)) {
                        for (var e : it) count++;
                    }
                    return count;
                });
            }

            // 7. Property Index
            if (targetPropKey != null) {
                final String k = targetPropKey;
                final String v = targetPropVal;
                benchmark("7. Index Lookup (" + k + "=" + v + ")", () -> {
                    long count = 0;
                    try (var it = db.getNodesByPropertyEquals(k, v)) {
                        for (var n : it) count++;
                    }
                    return count;
                });
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void benchmark(String testName, RunnableWithResult task) {
        System.out.print(String.format("%-50s", testName));
        
        long totalTime = 0;
        long itemsProcessed = 0;

        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            itemsProcessed = task.run(); 
            long end = System.nanoTime();
            totalTime += (end - start);
        }

        double avgTimeMs = (totalTime / (double) ITERATIONS) / 1_000_000.0;
        
        System.out.println(String.format(" | Avg: %8.3f ms | Items: %d", avgTimeMs, itemsProcessed));
    }

    @FunctionalInterface
    interface RunnableWithResult {
        long run(); 
        default RunnableWithResult wrap(Runnable r) {
            return () -> { r.run(); return 0; };
        }
    }
    
    private static void benchmark(String testName, Runnable task) {
        benchmark(testName, () -> { task.run(); return 0; });
    }
}