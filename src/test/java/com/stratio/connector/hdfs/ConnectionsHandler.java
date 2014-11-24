package com.stratio.connector.hdfs;

import com.stratio.connector.hdfs.connection.HDFSConnector;
import com.stratio.connector.hdfs.engine.HDFSMetadataEngine;
import com.stratio.connector.hdfs.engine.HDFSStorageEngine;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

public class ConnectionsHandler {
    protected final HDFSConnector hdfsConnector;

    public ConnectionsHandler() throws InitializationException {

        this.hdfsConnector = new HDFSConnector();
        this.hdfsConnector.init(null);
    }

    public void connect(ConnectorClusterConfig configuration) throws ConnectionException {

        this.hdfsConnector.connect(null, configuration);
    }

    public HDFSConnector getHDFSConnector() {

        return this.hdfsConnector;
    }

    public HDFSStorageEngine getStorageEngine() throws UnsupportedException {

        return (HDFSStorageEngine) this.hdfsConnector.getStorageEngine();
    }

    public HDFSMetadataEngine getMetadataEngine() throws UnsupportedException {

        return (HDFSMetadataEngine) this.hdfsConnector.getMetadataEngine();
    }

    public void close(ClusterName clusterName) throws ConnectionException {
        this.hdfsConnector.close(clusterName);
    }
}