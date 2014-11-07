package com.stratio.connector.hdfs.connection;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * Class implements native HDFS connection.
 *
 */
public class HDFSConnectionHandler extends ConnectionHandler {


    public HDFSConnectionHandler(IConfiguration configuration) {
        super(configuration);
    }

    /**
     * Use config & Credentials to create HDFS native connection.
     *
     * @param iCredentials
     *            .
     * @param connectorClusterConfig
     *            .
     *
     * @return DeepConnection.
     **/
    @Override
    protected Connection createNativeConnection(ICredentials iCredentials,
            ConnectorClusterConfig connectorClusterConfig) throws CreateNativeConnectionException {

        try {
            return new HDFSConnection(iCredentials,connectorClusterConfig);
        } catch (Exception e) {
            throw new CreateNativeConnectionException("Validation error", e);
        }
    }
}
