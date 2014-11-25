package com.stratio.connector.hdfs.engine;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.hdfs.connection.HDFSConnectionHandler;
import com.stratio.connector.hdfs.utils.HDFSClient;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;

@RunWith(PowerMockRunner.class)
public class HDFSStorageEngineTest {

    private static final ClusterName CLUSTERNAME_CONSTANT =  new ClusterName("cluster_name");
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String INDEX_NAME = "INDEX_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";
    private TableName tableMame = new TableName(INDEX_NAME, TYPE_NAME);
    private static final String ROW_NAME = "row_name";
    private static final String CELL_VALUE = "cell_value";

    private static final String CATALOG = "catalog";
    private static final String TABLE   = "table";

    @Mock
    private HDFSConnectionHandler connectionHandler;
    @Mock
    private Connection<HDFSClient> connection;
    @Mock
    private HDFSClient client;

    @Mock
    private HDFSStorageEngine hdfsStorageEngine;

    @Before
    public void before() throws HandlerConnectionException {

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        hdfsStorageEngine = new HDFSStorageEngine(connectionHandler);

    }
    @After
    public void after() throws Exception {
    }

    /**
     * Method: insert(ClusterName targetCluster, TableMetadata targetTable, Row row)
     */
    @Test
    public void testInsertOne() throws UnsupportedException, ExecutionException {

        ClusterName clusterName = CLUSTERNAME_CONSTANT;
        Row row = createRow(ROW_NAME, CELL_VALUE);

        TableName tableName = new TableName(CATALOG, TABLE);
        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Map indexex = Collections.EMPTY_MAP;
        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey   = Collections.EMPTY_LIST;

        TableMetadata targetTable = new TableMetadata(tableName, options, columns, indexex, clusterName,
                partitionKey, clusterKey);
        hdfsStorageEngine.insert(clusterName,targetTable,row);

        verify(client, times(1)).addRowToFile(any(String.class),anyString());
    }

    /**
     * Method: truncate(TableName targetTable, Connection connection)
     */
    @Test
    public void testTruncate() throws UnsupportedException, ExecutionException {

        TableName tableName = new TableName(CATALOG, TABLE);

        hdfsStorageEngine.truncate(tableName,connection);

        verify(client, times(1)).truncate(anyString());

    }



    private Row createRow(String rowKey, Object cellValue) {
        Cell cell = new Cell(cellValue);
        Row row = new Row(rowKey, cell);
        return row;
    }

}
