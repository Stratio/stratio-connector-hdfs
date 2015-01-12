package com.stratio.connector.hdfs.engine;

import java.util.Collection;
import java.util.Map;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.hdfs.connection.HDFSConnectionHandler;
import com.stratio.connector.hdfs.utils.HDFSClient;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Relation;

public class HDFSStorageEngine extends CommonsStorageEngine<HDFSClient> {

    public HDFSStorageEngine(HDFSConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    @Override
    protected void insert(TableMetadata tableMetadata, Row row, boolean isNotExists, Connection<HDFSClient> connection)
                    throws UnsupportedException, ExecutionException {

        if (isNotExists) {
            throw new UnsupportedException("Not yet supported");
        }
        HDFSClient hdfsClient = connection.getNativeConnection();

        Object cellValue;
        StringBuilder rowValues = new StringBuilder();
        for (Map.Entry<String, Cell> entry : row.getCells().entrySet()) {
            cellValue = entry.getValue().getValue();
            rowValues.append(cellValue.toString() + hdfsClient.getseparator());
        }

        String insert = rowValues.toString();
        if (hdfsClient.getseparator() != null && insert.endsWith(hdfsClient.getseparator())) {
            insert = insert.substring(0, insert.length() - 1) + "\n";
        }
        hdfsClient.addRowToFile(insert, tableMetadata.getName().getCatalogName() + "/"
                        + tableMetadata.getName().getName());

    }

    @Override
    protected void insert(TableMetadata tableMetadata, Collection<Row> collection, boolean isNotExists,
                    Connection<HDFSClient> connection) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");
    }

    @Override
    protected void truncate(TableName tableName, Connection<HDFSClient> connection) throws UnsupportedException,
                    ExecutionException {

        HDFSClient hdfsClient = connection.getNativeConnection();

        hdfsClient.truncate(tableName.getCatalogName() + "/" + tableName.getName());

    }

    @Override
    protected void delete(TableName tableName, Collection<Filter> whereClauses, Connection<HDFSClient> connection)
                    throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");
    }

    @Override
    protected void update(TableName tableName, Collection<Relation> assignments, Collection<Filter> whereClauses,
                    Connection<HDFSClient> connection) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");

    }

}
