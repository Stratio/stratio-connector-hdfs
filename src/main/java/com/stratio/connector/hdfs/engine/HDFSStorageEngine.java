package com.stratio.connector.hdfs.engine;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.hdfs.connection.HDFSConnectionHandler;
import com.stratio.connector.hdfs.utils.HDFSClient;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Relation;

public class HDFSStorageEngine extends CommonsStorageEngine<HDFSClient> {


    /**
     * The Log.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSClient.class);

    /**
     * Basic constructor.
     * 
     * @param connectionHandler
     */
    public HDFSStorageEngine(HDFSConnectionHandler connectionHandler) {
        super(connectionHandler);
    }



    @Override
    protected void insert(TableMetadata tableMetadata, Row row, boolean isNotExists, Connection<HDFSClient> connection)
            throws UnsupportedException, ExecutionException {

        if (isNotExists){
            throw new UnsupportedException("Not supported yet");
        }
        HDFSClient hdfsClient = connection.getNativeConnection();
        LinkedHashMap<ColumnName, ColumnMetadata> linkedColumns = null;

        String catalog = tableMetadata.getName().getCatalogName().getName();
        String tableName = tableMetadata.getName().getName();
        try {

            linkedColumns = (LinkedHashMap<ColumnName, ColumnMetadata>) hdfsClient.getMetaDataInfoFromFile
                    (catalog + "/" + tableName);

        } catch (IOException e) {
        		LOGGER.error("Error getting the metadata from com.stratio.connector.com.stratio.connector.hdfs.scala.com.stratio.connector.com.stratio.connector.hdfs.scala",e.getMessage());
        		throw new ExecutionException("Error getting the metadata from com.stratio.connector.com.stratio.connector.hdfs.scala.com.stratio.connector.com.stratio.connector.hdfs.scala", e);
        }

        String cellName;
        Object cellValue;
        StringBuilder rowValues = new StringBuilder();
        for (ColumnName columnName : linkedColumns.keySet()){
            for (Map.Entry<String, Cell> entry : row.getCells().entrySet()) {
                cellName = entry.getKey();

                cellValue = entry.getValue().getValue();
                ColumnName cName = new ColumnName(catalog, tableName, cellName);

                if(cName.getName().equals(columnName.getName())) {
                    rowValues.append((cellValue!=null?cellValue.toString():"") + hdfsClient.getseparator());
                }
            }
        }
        String insert = rowValues.toString();
        if (hdfsClient.getseparator()!=null && insert.endsWith(hdfsClient.getseparator())) {
            insert = insert.substring(0, insert.length() - 1) +"\n";
        }
        hdfsClient.addRowToFile(insert,tableMetadata.getName().getCatalogName()+"/"+tableMetadata
                .getName().getName());

    }


    @Override
    protected void insert(TableMetadata tableMetadata, Collection<Row> collection, boolean isNotExists,
            Connection<HDFSClient> connection) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");
    }

    @Override
    protected void truncate(TableName tableName, Connection<HDFSClient> connection)
            throws UnsupportedException, ExecutionException {

        HDFSClient hdfsClient = connection.getNativeConnection();

        hdfsClient.truncate(tableName.getCatalogName()+"/"+tableName.getName());

    }

    @Override
    protected void delete(TableName tableName, Collection<Filter> whereClauses,
            Connection<HDFSClient> connection) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");
    }

    @Override
    protected void update(TableName tableName, Collection<Relation> assignments,
            Collection<Filter> whereClauses, Connection<HDFSClient> connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");

    }




}
