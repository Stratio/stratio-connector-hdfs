package com.stratio.connector.hdfs.connection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.hdfs.configuration.HDFSConstants;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.security.ICredentials;

@RunWith(PowerMockRunner.class)
public class HDFSConnectionHandlerTest {

    private static final String CLUSTER_NAME = "cluster_name";
    private static final String HOST = "127.0.0.1";
    private static final String PORT = "9000";
    @Mock
    private IConfiguration        iConfiguration;
    private HDFSConnectionHandler connectionHandler;

    @Before
    public void before() throws Exception{
        iConfiguration = mock(IConfiguration.class);
        connectionHandler = new HDFSConnectionHandler(iConfiguration);
    }

    /**
     * Method: createConnection(String clusterName, Connection connection)
     */
    @Test
    public void testCreateConnection() throws Exception, HandlerConnectionException {

        ICredentials credentials = mock(ICredentials.class);
        credentials = null;
        Map<String, String> options = new HashMap<>();
        options.put(HDFSConstants.HOST, HDFSConstants.HDFS_URI_SCHEME+"://"+HOST);
        options.put(HDFSConstants.HOSTS, "127.0.0.1 , 127.0.0.2");
        options.put(HDFSConstants.PORT, PORT);
        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName(CLUSTER_NAME), options,options);

        HDFSConnection connection = mock(HDFSConnection.class);
        whenNew(HDFSConnection.class).withArguments(credentials, config).thenReturn(connection);

        connectionHandler.createConnection(credentials, config);

        Map<String, HDFSConnection> mapConnection = (Map<String, HDFSConnection>) Whitebox.getInternalState(
                connectionHandler, "connections");


        HDFSConnection recoveredConnection = mapConnection.get(CLUSTER_NAME);

        assertNotNull("The connection is not null", recoveredConnection);
        //assertSame("The recoveredConnection is correct", connection, recoveredConnection);

    }


    @Test
    public void testCloseConnection() throws Exception {

        Map<String, HDFSConnection> mapConnection = (Map<String, HDFSConnection>) Whitebox.getInternalState(
                connectionHandler, "connections");
        HDFSConnection connection = mock(HDFSConnection.class);
        mapConnection.put(CLUSTER_NAME, connection);

        connectionHandler.closeConnection(CLUSTER_NAME);

        assertFalse(mapConnection.containsKey(CLUSTER_NAME));
        verify(connection, times(1)).close();
    }

    @Test
    public void testGetConnection() throws HandlerConnectionException {
        Map<String, HDFSConnection> mapConnection = (Map<String, HDFSConnection>) Whitebox
                .getInternalState(connectionHandler, "connections");

        HDFSConnection connection = mock(HDFSConnection.class);
        mapConnection.put(CLUSTER_NAME, connection);

        Connection recoveredConnection = connectionHandler.getConnection(CLUSTER_NAME);
        assertNotNull("The connection is not null", recoveredConnection);
        assertSame   ("The connection is correct" , connection, recoveredConnection);

    }

}
