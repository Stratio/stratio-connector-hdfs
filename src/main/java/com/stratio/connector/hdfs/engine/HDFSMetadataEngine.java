package com.stratio.connector.hdfs.engine;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.CommonsMetadataEngine;
import com.stratio.connector.hdfs.connection.HDFSConnectionHandler;
import com.stratio.connector.hdfs.utils.HDFSClient;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;

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
        connection.getNativeConnection().createMetaDataFile(tableMetadata);
        connection.getNativeConnection().addFile(tableMetadata.getName().getCatalogName().getName()+"/"+
                tableMetadata.getName().getName());
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

    @Override
    protected void alterTable(TableName name, com.stratio.crossdata.common.data.AlterOptions alterOptions,
            Connection<HDFSClient> connection) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");

    }
}
