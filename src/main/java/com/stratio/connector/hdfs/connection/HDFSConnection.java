package com.stratio.connector.hdfs.connection;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.hdfs.utils.HDFSClient;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * .Connection object exist in the ConnectionHandler and contains all the HDFS connection info & config.
 *  {@link com.stratio.connector.commons.connection.Connection}
 *
 */
public class HDFSConnection extends Connection {

    /**
     * The Log.
     */
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private HDFSClient hdfsClient = null;
    private boolean isConnected   = false;


    public HDFSConnection(ICredentials credentials, ConnectorClusterConfig config)throws
            CreateNativeConnectionException {

        Map<String, String> clusterOptions = config.getOptions();

        if(credentials == null){
            //TODO Add the configuration params to the HDFS Client Configuration
            hdfsClient = new HDFSClient(config);
            isConnected = true;
            if(LOGGER.isInfoEnabled()){
                LOGGER.info("New HDFS connection established");
            }

        }else {
            throw new CreateNativeConnectionException("Credentials are not supported yet");
        }

    }

    @Override
    public void close() {

        if( hdfsClient != null ){
            hdfsClient = null;
        }
        if(LOGGER.isInfoEnabled()) {
            LOGGER.info(" HDFS connection closed");
        }
        isConnected = false;

    }

    @Override
    public boolean isConnect() {
        return isConnected;
    }

    @Override
    public Object getNativeConnection() {
        return hdfsClient;
    }
}
