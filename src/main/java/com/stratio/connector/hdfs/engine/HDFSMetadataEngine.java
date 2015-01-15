package com.stratio.connector.hdfs.engine;

import java.util.List;
import java.util.Map;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.CommonsMetadataEngine;
import com.stratio.connector.hdfs.connection.HDFSConnectionHandler;
import com.stratio.connector.hdfs.utils.HDFSClient;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;

public class HDFSMetadataEngine extends CommonsMetadataEngine<HDFSClient>{


    public HDFSMetadataEngine(HDFSConnectionHandler connectionHandler) {
        super(connectionHandler);
    }



    @Override
    protected void createCatalog(CatalogMetadata catalogMetadata, Connection<HDFSClient> connection)
            throws UnsupportedException, ExecutionException {

        connection.getNativeConnection().mkdir(catalogMetadata.getName().getName());
    }

    @Override
    protected void createTable(TableMetadata tableMetadata, Connection<HDFSClient> connection)
            throws UnsupportedException, ExecutionException {
        connection.getNativeConnection().addFile(tableMetadata.getName().getCatalogName().getName()+"/"+
                tableMetadata.getName().getName());
        connection.getNativeConnection().createMetaDataFile(tableMetadata);

    }

    @Override
    protected void dropCatalog(CatalogName catalogName, Connection<HDFSClient> connection)
            throws UnsupportedException, ExecutionException {

        connection.getNativeConnection().deleteFile(catalogName.getName());

    }

    @Override
    protected void dropTable(TableName tableName, Connection<HDFSClient> connection)
            throws UnsupportedException, ExecutionException {

        connection.getNativeConnection().deleteFile(tableName.getCatalogName().getName()+"/"+
                tableName.getName());
    }

    @Override
    protected void createIndex(IndexMetadata indexMetadata, Connection connection)
            throws UnsupportedException, ExecutionException {

        throw new UnsupportedException("Not yet supported");
    }

    @Override
    protected void dropIndex(IndexMetadata indexMetadata, Connection connection)
            throws UnsupportedException, ExecutionException {

        throw new UnsupportedException("Not yet supported");
    }

    @Override protected List<CatalogMetadata> provideMetadata(ClusterName clusterName,
            Connection<HDFSClient> connection) throws ConnectorException {
        throw new UnsupportedException("Not yet supported");
    }

    @Override protected CatalogMetadata provideCatalogMetadata(CatalogName catalogName, ClusterName clusterName,
            Connection<HDFSClient> connection) throws ConnectorException {
        throw new UnsupportedException("Not yet supported");
    }

    @Override protected TableMetadata provideTableMetadata(TableName tableName, ClusterName clusterName,
            Connection<HDFSClient> connection) throws ConnectorException {
        throw new UnsupportedException("Not yet supported");
    }

    @Override protected void alterCatalog(CatalogName catalogName, Map<Selector, Selector> map,
            Connection<HDFSClient> connection) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");

    }

    @Override
    protected void alterTable(TableName name, com.stratio.crossdata.common.data.AlterOptions alterOptions,
            Connection<HDFSClient> connection) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");

    }
}
