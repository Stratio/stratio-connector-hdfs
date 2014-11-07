package com.stratio.connector.hdfs.connection;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.CommonsConnector;
import com.stratio.connector.commons.util.ManifestUtil;
import com.stratio.connector.hdfs.engine.HDFSMetadataEngine;
import com.stratio.connector.hdfs.engine.HDFSQueryEngine;
import com.stratio.connector.hdfs.engine.HDFSStorageEngine;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.connectors.ConnectorApp;

public class HDFSConnector extends CommonsConnector {

    /**
     * The Log.
     */
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    /**
     * The connector name.
     */
    private String connectorName;
    /**
     * The datastore name.
     */
    private String[] datastoreName;

    private Configuration HDFSConfiguration;

    public HDFSConnector() throws InitializationException {
        connectorName = ManifestUtil.getConectorName("HDFSConnector.xml");
        datastoreName = ManifestUtil.getDatastoreName("HDFSConnector.xml");

    }
//    public HDFSConnector(ICredentials credentials, ConnectorClusterConfig config) throws InitializationException {
//        connectorName = ManifestUtil.getConectorName("HDFSConnector.xml");
//        datastoreName = ManifestUtil.getDatastoreName("HDFSConnector.xml");
//        if(credentials==null){
//
//        }
//
//    }


    @Override
    public String getConnectorName() {
        return connectorName;
    }

    @Override
    public String[] getDatastoreName() {
        return datastoreName;
    }

    @Override
    public void init(IConfiguration configuration) throws InitializationException {
        connectionHandler = new HDFSConnectionHandler(configuration);
    }

    @Override
    public IStorageEngine getStorageEngine() throws UnsupportedException {
        return new HDFSStorageEngine((HDFSConnectionHandler)connectionHandler);
    }

    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {
        return new HDFSQueryEngine((HDFSConnectionHandler)connectionHandler);
    }

    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {
        return new HDFSMetadataEngine((HDFSConnectionHandler)connectionHandler);
    }

    public static void main(String[] args) throws InitializationException {
        HDFSConnector hdfsConnector = new HDFSConnector();
        ConnectorApp connectorApp = new ConnectorApp();
        connectorApp.startup(hdfsConnector);
    }
}
