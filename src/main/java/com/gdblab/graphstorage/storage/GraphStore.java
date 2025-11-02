package com.gdblab.graphstorage.storage;

import org.rocksdb.*;

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
    public static final String CF_INDEX = "cf_index";
    public static final String CF_META = "cf_meta";

    private final RocksDB db;
    private final ColumnFamilyHandle cfDefault;
    private final ColumnFamilyHandle cfNodes;
    private final ColumnFamilyHandle cfEdges;
    private final ColumnFamilyHandle cfIndex;
    private final ColumnFamilyHandle cfMeta;

    // Servicios
    private final NodeStore nodeStore;
    private final EdgeStore edgeStore;
    private final IndexStore indexStore;
    private final GraphQueries queries;
    private final GraphIngestor ingestor;
    private final MetaStore metaStore;

    private GraphStore(RocksDB db,
                       ColumnFamilyHandle cfDefault,
                       ColumnFamilyHandle cfNodes,
                       ColumnFamilyHandle cfEdges,
                       ColumnFamilyHandle cfIndex,
                       ColumnFamilyHandle cfMeta) {
        this.db = db;
        this.cfDefault = cfDefault;
        this.cfNodes = cfNodes;
        this.cfEdges = cfEdges;
        this.cfIndex = cfIndex;
        this.cfMeta = cfMeta;

        this.nodeStore = new NodeStore(db, cfNodes);
        this.edgeStore = new EdgeStore(db, cfEdges);
        this.indexStore = new IndexStore(db, cfIndex);
        this.metaStore = new MetaStore(db, cfMeta);
        this.queries = new GraphQueries(nodeStore, edgeStore, indexStore, metaStore);
        this.ingestor = new GraphIngestor(nodeStore, edgeStore, indexStore, metaStore);
    }

    // Abrir o crear si es necesario la base de datos 
   public static GraphStore open(Path dbPath) throws RocksDBException, IOException {
    RocksDB.loadLibrary();
    Files.createDirectories(dbPath);

BlockBasedTableConfig tableCfg = new BlockBasedTableConfig()
    .setCacheIndexAndFilterBlocks(true)
    .setEnableIndexCompression(true);
ColumnFamilyOptions cfAll = new ColumnFamilyOptions()
    .setCompressionType(CompressionType.LZ4_COMPRESSION)
    .setBottommostCompressionType(CompressionType.LZ4_COMPRESSION)
    .setTableFormatConfig(tableCfg);




List<ColumnFamilyDescriptor> cfds = List.of(
    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
    new ColumnFamilyDescriptor(CF_NODES.getBytes(StandardCharsets.UTF_8), cfAll),
    new ColumnFamilyDescriptor(CF_EDGES.getBytes(StandardCharsets.UTF_8), cfAll),
    new ColumnFamilyDescriptor(CF_INDEX.getBytes(StandardCharsets.UTF_8), cfAll),
    new ColumnFamilyDescriptor(CF_META.getBytes(StandardCharsets.UTF_8), cfAll)
);

    List<ColumnFamilyHandle> handles = new ArrayList<>();

    DBOptions dbo = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);

    RocksDB db = RocksDB.open(dbo, dbPath.toString(), cfds, handles);
    return new GraphStore(db, handles.get(0), handles.get(1), handles.get(2), handles.get(3), handles.get(4));
}


    public NodeStore nodes() { return nodeStore; }
    public EdgeStore edges() { return edgeStore; }
    public GraphQueries queries() { return queries; }
    public GraphIngestor ingestor() { return ingestor; }
    public MetaStore meta() { return metaStore; }

    @Override
    public void close() {
        safeClose(ingestor);
        safeClose(queries);
        safeClose(indexStore);
        safeClose(edgeStore);
        safeClose(nodeStore);
        safeClose(metaStore);
        try { cfIndex.close(); } catch (Exception ignore) {}
        try { cfMeta.close(); } catch (Exception ignore) {}
        try { cfEdges.close(); } catch (Exception ignore) {}
        try { cfNodes.close(); } catch (Exception ignore) {}
        try { cfDefault.close(); } catch (Exception ignore) {}
        try { db.close(); } catch (Exception ignore) {}
    }

    private static void safeClose(Object o) {
        if (o instanceof Closeable c) { try { c.close(); } catch (IOException ignore) {} }
    }
}

