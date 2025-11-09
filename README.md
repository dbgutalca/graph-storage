# Welcome to GraphStorage 

GraphStorage is a Java library for storing large-scale property graphs in secondary memory (HDD/SSD). It is built on top of RocksDB to provide a fast, persistent, and efficient storage layer.

While this library was developed as a core component for the PathDB graph database engine, it is designed as a standalone module that can be integrated into any Java project requiring persistent graph data structures.

## Academic Context

This project is developed as part of a final-year Computer Engineering thesis at [REPLACE: Your University Name]. The primary goal is to provide a robust, high-performance on-disk storage implementation for comparative analysis within the PathDB research project.

## Features

- Persistent Storage: Leverages RocksDB for a fast and durable key-value store, optimized for modern SSDs.

- Bulk Ingestion: Optimized methods for loading large `.pgdf` datasets quickly.

- Lazy Iteration: All collection-based queries (getNodeIterator, getNeighbours, etc.) return AutoCloseableIterables, ensuring low memory overhead by streaming data directly from disk.

- Query API: Provides methods for single-entity lookups (by ID), property-based searches, and neighbor retrieval.

- Schema & Metadata: Includes helpers to query graph-wide metadata (e.g., node/edge counts) and infer the graph's structure.


Full Documentation

This README serves as a brief overview. For a complete guide and detailed API definitions, please see the full documentation available at: 

[GraphStorage Documentation](https://urzuaf.github.io/graph-storage-docs/)