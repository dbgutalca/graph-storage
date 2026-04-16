package com.gdblab.graphstorage;

import com.gdblab.graphstorage.storage.Utils.GraphStorageException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility program to measure ingestion performance.
 */
public class StorageLoader {

    public static void main(String[] args) {
        Path dbPath = Paths.get("./graph_db");
        String nodesFile = "Nodes.pgdf";
        String edgesFile = "Edges.pgdf";

        System.out.println("--- GraphStorage Ingestion Tool ---");
        System.out.println("Target Database: " + dbPath.toAbsolutePath());

        try (GraphStorage db = GraphStorage.open(dbPath)) {
            
            System.out.println("Starting node ingestion...");
            long startNodes = System.currentTimeMillis();
            
            db.insertNodesByFile(nodesFile);
            
            long endNodes = System.currentTimeMillis();
            System.out.println("Nodes ingested in: " + (endNodes - startNodes) + " ms");

            System.out.println("Starting edge ingestion...");
            long startEdges = System.currentTimeMillis();
            
            db.insertEdgesByFile(edgesFile);
            
            long endEdges = System.currentTimeMillis();
            System.out.println("Edges ingested in: " + (endEdges - startEdges) + " ms");

            System.out.println("\n--- Verification ---");
            System.out.println("Total Nodes: " + db.getNodesQuantity());
            System.out.println("Total Edges: " + db.getEdgesQuantity());
            System.out.println("Full Load Process Completed Successfully.");

        } catch (GraphStorageException e) {
            System.err.println("CRITICAL ERROR during ingestion:");
            e.printStackTrace();
        }
    }
}
