package com.stratio.connector.hdfs.engine.query;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.hdfs.ConnectionsHandler;
import com.stratio.connector.hdfs.configuration.HDFSConstants;
import com.stratio.connector.hdfs.engine.HDFSStorageEngine;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;

public class HDFSSimpleInsertFT {


    private static final String INDEX_NAME = "INDEX_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";
    private static final String COLUMN_1 = "column1";
    private static final String COLUMN_2 = "column2";
    private static final String COLUMN_3 = "column3";
    private static final String COLUMN_4 = "column4";
    private static final Object VALUE_1  = "value1";
    private static final Object VALUE_2  = "value2";
    private static final Object VALUE_3  = "value3";
    private static final Object VALUE_4  = "value4";

    private TableName tableMame = new TableName(INDEX_NAME, TYPE_NAME);
    private static final String ROW_NAME = "row_name";
    private static final String CELL_VALUE = "cell_value";

    private static final ClusterName CLUSTERNAME_CONSTANT =  new ClusterName("cluster_name");
    private static final String HOST    = "127.0.0.1";
    private static final String PORT    = "9000";
    private static final String PATH    = "/user/dgomez/test/songs.csv";
    private static final String CATALOG = "test";
    private static final String TABLE   = "songs";


    private static HDFSStorageEngine hdfsStorageEngine;

    @Before
    public void before() throws InitializationException, ConnectionException, UnsupportedException {

        ConnectionsHandler connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(prepareConfiguration());
        hdfsStorageEngine = connectionBuilder.getStorageEngine();

    }
    /**
     * Method: insert(ClusterName targetCluster, TableMetadata targetTable, Row row)
     */
    @Test
    public void testInsertOne()
            throws UnsupportedException, ExecutionException {

        ClusterName clusterName = CLUSTERNAME_CONSTANT;
        Row row = createRow();

        TableName tableName = new TableName(CATALOG, TABLE);
        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Map indexex = Collections.EMPTY_MAP;
        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey   = Collections.EMPTY_LIST;

        TableMetadata targetTable = new TableMetadata(tableName, options, columns, indexex, clusterName,
                partitionKey, clusterKey);
        hdfsStorageEngine.insert(clusterName,targetTable,row);
    }

    private Row createRow(String rowKey, Object cellValue) {
        Cell cell = new Cell(cellValue);
        Row row = new Row(rowKey,cell);

        return row;
    }
    private Row createRow() {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();

        cells.put(COLUMN_1, new Cell(VALUE_1));
        cells.put(COLUMN_2, new Cell(VALUE_2));
        cells.put(COLUMN_3, new Cell(VALUE_3));
        cells.put(COLUMN_4, new Cell(VALUE_4));

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
        options.put(HDFSConstants.CONFIG_DIFERENT_PARTITIONS, "false");
        options.put(HDFSConstants.CONFIG_PARTITION_NAME, "partition");
        options.put(HDFSConstants.CONFIG_EXTENSION_NAME, ".csv");

        ConnectorClusterConfig configuration = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT, options,options);

        return configuration;
    }
}
