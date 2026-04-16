package com.gdblab.graphstorage.storage;

import org.rocksdb.*;
import com.gdblab.graphstorage.engine.NodeStore;
import com.gdblab.graphstorage.engine.EdgeStore;
import com.gdblab.graphstorage.engine.IndexStore;
import com.gdblab.graphstorage.engine.MetaStore;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class GraphStore implements AutoCloseable {

    // Column family names
    public static final String CF_NODES = "cf_nodes";
    public static final String CF_EDGES = "cf_edges";
    public static final String CF_IDX_NODE_PROP = "cf_idx_node_prop";
    public static final String CF_IDX_EDGE_PROP = "cf_idx_edge_prop";
    public static final String CF_IDX_EDGE_SRC = "cf_idx_edge_src";
    public static final String CF_IDX_EDGE_DST = "cf_idx_edge_dst";
    public static final String CF_IDX_LABEL = "cf_idx_label";

    private final RocksDB db;
    private final ColumnFamilyHandle cfDefault;
    private final ColumnFamilyHandle cfNodes;
    private final ColumnFamilyHandle cfEdges;
    private final ColumnFamilyHandle cfIdxNodeProp;
    private final ColumnFamilyHandle cfIdxEdgeProp;
    private final ColumnFamilyHandle cfIdxEdgeSrc;
    private final ColumnFamilyHandle cfIdxEdgeDst;
    private final ColumnFamilyHandle cfIdxLabel;

    private final NodeStore nodeStore;
    private final EdgeStore edgeStore;
    private final IndexStore indexStore;
    private final GraphQueries queries;
    private final GraphIngestor ingestor;
    private final MetaStore metaStore;
    private final Path dbPath;

    private GraphStore(RocksDB db,
            ColumnFamilyHandle cfDefault,
            ColumnFamilyHandle cfNodes,
            ColumnFamilyHandle cfEdges,
            ColumnFamilyHandle cfIdxNodeProp,
            ColumnFamilyHandle cfIdxEdgeProp,
            ColumnFamilyHandle cfIdxEdgeSrc,
            ColumnFamilyHandle cfIdxEdgeDst,
            ColumnFamilyHandle cfIdxLabel,
            MetaStore metaStore,
            Path dbPath) {
        this.db = db;
        this.cfDefault = cfDefault;
        this.cfNodes = cfNodes;
        this.cfEdges = cfEdges;
        this.cfIdxNodeProp = cfIdxNodeProp;
        this.cfIdxEdgeProp = cfIdxEdgeProp;
        this.cfIdxEdgeSrc = cfIdxEdgeSrc;
        this.cfIdxEdgeDst = cfIdxEdgeDst;
        this.cfIdxLabel = cfIdxLabel;
        this.metaStore = metaStore;
        this.dbPath = dbPath;

        this.nodeStore = new NodeStore(db, cfNodes);
        this.edgeStore = new EdgeStore(db, cfEdges);
        this.indexStore = new IndexStore(db, cfIdxNodeProp, cfIdxEdgeProp, cfIdxEdgeSrc, cfIdxEdgeDst, cfIdxLabel);
        this.queries = new GraphQueries(nodeStore, edgeStore, indexStore, this.metaStore);
        this.ingestor = new GraphIngestor(db, cfNodes, cfEdges, cfIdxNodeProp, cfIdxEdgeProp, cfIdxEdgeSrc, cfIdxEdgeDst, cfIdxLabel, this.metaStore);
    }

    public static GraphStore open(Path dbPath) throws RocksDBException, IOException {
    RocksDB.loadLibrary();
    Files.createDirectories(dbPath);

    BlockBasedTableConfig tableConfig = null;
    ColumnFamilyOptions cfAll = null;
    ColumnFamilyOptions default_cfo = null;
    DBOptions dbo = null;
    List<ColumnFamilyDescriptor> cfds = null;
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    RocksDB db = null;

    try {

        tableConfig = new BlockBasedTableConfig();
        tableConfig.setCacheIndexAndFilterBlocks(true)
                   .setEnableIndexCompression(true);

        cfAll = new ColumnFamilyOptions();
        cfAll.setCompressionType(CompressionType.LZ4_COMPRESSION) 
             .setBottommostCompressionType(CompressionType.LZ4_COMPRESSION)
             .setTableFormatConfig(tableConfig)
             .setWriteBufferSize(128*1014*1024) //128MB per memtable
             .setMaxWriteBufferNumber(4)
             .setMinWriteBufferNumberToMerge(2);
              

        default_cfo = new ColumnFamilyOptions(); 

        cfds = List.of(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, default_cfo),
                new ColumnFamilyDescriptor(CF_NODES.getBytes(StandardCharsets.UTF_8), cfAll),
                new ColumnFamilyDescriptor(CF_EDGES.getBytes(StandardCharsets.UTF_8), cfAll),
                new ColumnFamilyDescriptor(CF_IDX_NODE_PROP.getBytes(StandardCharsets.UTF_8), cfAll),
                new ColumnFamilyDescriptor(CF_IDX_EDGE_PROP.getBytes(StandardCharsets.UTF_8), cfAll),
                new ColumnFamilyDescriptor(CF_IDX_EDGE_SRC.getBytes(StandardCharsets.UTF_8), cfAll),
                new ColumnFamilyDescriptor(CF_IDX_EDGE_DST.getBytes(StandardCharsets.UTF_8), cfAll),
                new ColumnFamilyDescriptor(CF_IDX_LABEL.getBytes(StandardCharsets.UTF_8), cfAll));

        dbo = new DBOptions(); 
        dbo.setCreateIfMissing(true) 
           .setCreateMissingColumnFamilies(true)
           .setMaxBackgroundJobs(Runtime.getRuntime().availableProcessors());

        db = RocksDB.open(dbo, dbPath.toString(), cfds, handles);

        MetaStore metaStore = MetaStore.load(dbPath);

        return new GraphStore(db, 
                handles.get(0), // default
                handles.get(1), // nodes
                handles.get(2), // edges
                handles.get(3), // idx_node_prop
                handles.get(4), // idx_edge_prop
                handles.get(5), // idx_edge_src
                handles.get(6), // idx_edge_dst
                handles.get(7), // idx_label
                metaStore,
                dbPath);
        
    } catch (RocksDBException | IOException e) {
        for (ColumnFamilyHandle handle : handles) {
            try { handle.close(); } catch (Exception ignore) {}
        }
        if (db != null) {
            try { db.close(); } catch (Exception ignore) {}
        }
        throw e;

    } finally {
        if (dbo != null) {
            dbo.close();
        }
        if (cfAll != null) {
            cfAll.close();
        }
        if (default_cfo != null) {
            default_cfo.close();
        }
    }
}

    public NodeStore nodes() {
        return nodeStore;
    }

    public EdgeStore edges() {
        return edgeStore;
    }

    public GraphQueries queries() {
        return queries;
    }

    public GraphIngestor ingestor() {
        return ingestor;
    }

    public MetaStore meta() {
        return metaStore;
    }

    @Override
    public void close() {
        try {
            metaStore.save(dbPath);
        } catch (IOException e) {
            System.err.println("CRITICAL: Failed to save metadata on close: " + e.getMessage());
            e.printStackTrace();
        }

        safeClose(ingestor);
        safeClose(queries);
        safeClose(indexStore);
        safeClose(edgeStore);
        safeClose(nodeStore);

        try { cfIdxLabel.close(); } catch (Exception ignore) {}
        try { cfIdxEdgeDst.close(); } catch (Exception ignore) {}
        try { cfIdxEdgeSrc.close(); } catch (Exception ignore) {}
        try { cfIdxEdgeProp.close(); } catch (Exception ignore) {}
        try { cfIdxNodeProp.close(); } catch (Exception ignore) {}
        try { cfEdges.close(); } catch (Exception ignore) {}
        try { cfNodes.close(); } catch (Exception ignore) {}
        try { cfDefault.close(); } catch (Exception ignore) {}
        try {
            db.close();
        } catch (Exception ignore) {
        }
    }

    private static void safeClose(Object o) {
        if (o instanceof Closeable c) {
            try {
                c.close();
            } catch (IOException ignore) {
            }
        }
    }
}
