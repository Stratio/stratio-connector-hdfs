package com.stratio.connector.hdfs.engine.query;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.hdfs.ConnectionsHandler;
import com.stratio.connector.hdfs.configuration.HDFSConstants;
import com.stratio.connector.hdfs.engine.HDFSStorageEngine;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;

public class HDFSSimpleInsertFT {



    private static final String INDEX_NAME = "INDEX_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";

    private static final String COLUMN_1 = "id";
    private static final String COLUMN_2 = "author";
    private static final String COLUMN_3 = "title";
    private static final String COLUMN_4 = "year";
    private static final String COLUMN_5 = "length";
    private static final String COLUMN_6 = "Single";

    private static final Object VALUE_1  = "1";
    private static final Object VALUE_2  = "Guns Roses";
    private static final Object VALUE_3  = "Aint it fun";
    private static final Object VALUE_4  = "1995";
    private static final Object VALUE_5  = "360";
    private static final Object VALUE_6  = "Greatest Hits";



    private static final ClusterName CLUSTERNAME_CONSTANT =  new ClusterName("cluster_name");
    private static final String HOST    = "10.200.0.60";
    private static final String PORT    = "9000";

    private static final String CATALOG = "test";
    private static final String TABLE   = "songs";


    private static HDFSStorageEngine hdfsStorageEngine;

    @Before
    public void before() throws InitializationException, ConnectionException, UnsupportedException  {

        ConnectionsHandler connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(prepareConfiguration());
        hdfsStorageEngine = connectionBuilder.getStorageEngine();

        Map<TableName, TableMetadata> catalogMetadata =  Collections.EMPTY_MAP;
        try {
            connectionBuilder.getMetadataEngine().createCatalog(CLUSTERNAME_CONSTANT,
                    new CatalogMetadata(new CatalogName(CATALOG), Collections.EMPTY_MAP, catalogMetadata));
            connectionBuilder.getMetadataEngine().createTable(CLUSTERNAME_CONSTANT, createTableMetadta());
        }catch(ExecutionException e){}

    }
    /**
     * Method: insert(ClusterName targetCluster, TableMetadata targetTable, Row row)
     */
    @Test
    public void testInsertOne()
            throws UnsupportedException, ExecutionException {


        Row row = createRow();

        TableMetadata targetTable = createTableMetadta();
        hdfsStorageEngine.insert(CLUSTERNAME_CONSTANT,targetTable,row,false);
    }

    private TableMetadata createTableMetadta() {
        TableName tableName = new TableName(CATALOG, TABLE);
        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        Map indexex = Collections.EMPTY_MAP;
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        LinkedList<ColumnName> clusterKey   = new LinkedList<>();

        return new TableMetadata(tableName, options, columns, indexex, CLUSTERNAME_CONSTANT,
                partitionKey, clusterKey);
    }

    private Row createRow() {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();

        cells.put(COLUMN_1, new Cell(VALUE_1));
        cells.put(COLUMN_2, new Cell(VALUE_2));
        cells.put(COLUMN_3, new Cell(VALUE_3));
        cells.put(COLUMN_4, new Cell(VALUE_4));
        cells.put(COLUMN_5, new Cell(VALUE_5));
        cells.put(COLUMN_6, new Cell(VALUE_6));

        row.setCells(cells);
        return row;
    }

    /**
     * Create the configuration object to config the connector cluster information
     *
     * @return Cluster configuration object
     */
    private static ConnectorClusterConfig prepareConfiguration() {

        Map<String, String> options = new HashMap<>();
        options.put(HDFSConstants.HOST, HOST);
        options.put(HDFSConstants.PORT, PORT);
        options.put(HDFSConstants.CONFIG_DIFFERENT_PARTITIONS, "false");
        options.put(HDFSConstants.CONFIG_PARTITION_NAME, "partition");
        options.put(HDFSConstants.CONFIG_EXTENSION_NAME, ".csv");

        ConnectorClusterConfig configuration = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT, options,options);

        return configuration;
    }
}
