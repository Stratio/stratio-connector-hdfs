package com.stratio.connector.hdfs.engine;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
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
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;

@RunWith(PowerMockRunner.class)
public class HDFSMetadataEngineTest {

    private static final ClusterName CLUSTERNAME_CONSTANT =  new ClusterName("cluster_name");
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String CATALOG = "catalog";
    private static final String INDEX_NAME = "INDEX_NAME".toLowerCase();
    private static final String TYPE_NAME = "TYPE_NAME".toLowerCase();
    private TableName tableName = new TableName(INDEX_NAME, TYPE_NAME);

    @Mock
    private HDFSConnectionHandler connectionHandler;
    @Mock
    private Connection<HDFSClient> connection;
    @Mock
    private HDFSClient client;

    @Mock
    private HDFSMetadataEngine hdfsMetadataEngine;

    @Before
    public void before() throws HandlerConnectionException {

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        hdfsMetadataEngine = new HDFSMetadataEngine(connectionHandler);

    }
    @After
    public void after() throws Exception {
    }

    @Test
    public void createCatalogTest() throws UnsupportedException, ExecutionException {

        hdfsMetadataEngine.createCatalog(new ClusterName("cluster_name"),new CatalogMetadata(new CatalogName(CATALOG), null, null));
        verify(client, times(1)).mkdir(anyString());

    }

    @Test
    public void dropCatalogTest() throws UnsupportedException, ExecutionException {

        hdfsMetadataEngine.dropCatalog(new CatalogName(CATALOG), connection);
        verify(client, times(1)).deleteFile(anyString());

    }
    @Test
    public void createTableTest() throws UnsupportedException, ExecutionException {


        TableMetadata tablemetadta = mock(TableMetadata.class);
        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Map indexex = Collections.EMPTY_MAP;
        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey   = Collections.EMPTY_LIST;

        hdfsMetadataEngine.createTable(new ClusterName("cluster_name"),
                new TableMetadata(tableName, options, columns, indexex, CLUSTERNAME_CONSTANT, partitionKey, clusterKey));

        verify(client, times(1)).addFile(anyString());
    }

    @Test
    public void dropTableTest() throws UnsupportedException, ExecutionException {

        hdfsMetadataEngine.dropTable(tableName,connection);

        verify(client, times(1)).deleteFile(anyString());
    }
}
