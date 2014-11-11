package com.stratio.connector.hdfs;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.hdfs.configuration.HDFSConstants;
import com.stratio.connector.hdfs.engine.HDFSMetadataEngine;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;

@RunWith(PowerMockRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HDFSConnectorMetadataEngineFT {


    private static final ClusterName CLUSTERNAME_CONSTANT =  new ClusterName("cluster_name");
    private static final String HOST    = "127.0.0.1";
    private static final String PORT    = "9000";
    private static final String CATALOG = "catalog";
    private static final String TABLE   = "table";


    private static HDFSMetadataEngine hdfsMetadataEngine;

    @Before
    public void before() throws InitializationException, ConnectionException, UnsupportedException {

        ConnectionsHandler connectionBuilder = new ConnectionsHandler();
        connectionBuilder.connect(prepareConfiguration());
        hdfsMetadataEngine = connectionBuilder.getMetadataEngine();

    }

    @Test
    public void test1_createCatalog () throws UnsupportedException, ExecutionException {

          hdfsMetadataEngine.createCatalog(new ClusterName("cluster_name"),new CatalogMetadata(new CatalogName(CATALOG), null, null));

    }



    @Test
    public void test2_createTable() throws UnsupportedException, ExecutionException {

        TableName tableName = new TableName(CATALOG, TABLE);
        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Map indexex = Collections.EMPTY_MAP;
        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey   = Collections.EMPTY_LIST;
        ClusterName clusterRef = getClusterName();

        hdfsMetadataEngine.createTable( new ClusterName("cluster_name"),
                new TableMetadata(tableName, options, columns, indexex, clusterRef, partitionKey, clusterKey));

    }

    @Test
    public void test3_dropTable () throws UnsupportedException, ExecutionException {

        hdfsMetadataEngine.dropTable(new ClusterName("cluster_name"), new TableName(CATALOG, TABLE));

    }

    @Test
    public void test4_dropCatalog () throws UnsupportedException, ExecutionException {

        hdfsMetadataEngine.dropCatalog(new ClusterName("cluster_name"),new CatalogName(CATALOG));

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
        options.put(HDFSConstants.CONFIG_DIFERENT_PARTITIONS, "true");
        ConnectorClusterConfig configuration = new ConnectorClusterConfig(CLUSTERNAME_CONSTANT, options);

        return configuration;
    }

    private ClusterName getClusterName() {
        return new ClusterName(CATALOG + "-" + TABLE);
    }

}
