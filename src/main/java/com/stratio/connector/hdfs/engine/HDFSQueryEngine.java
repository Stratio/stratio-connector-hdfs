package com.stratio.connector.hdfs.engine;

import com.stratio.connector.commons.engine.CommonsQueryEngine;
import com.stratio.connector.hdfs.connection.HDFSConnectionHandler;
import com.stratio.connector.hdfs.engine.query.QueryExecutor;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.result.QueryResult;

public class HDFSQueryEngine extends CommonsQueryEngine {

    private final HDFSConnectionHandler connectionHandler;

    public HDFSQueryEngine(HDFSConnectionHandler connectionHandler) {
        super(connectionHandler);
        this.connectionHandler = connectionHandler;
    }

    @Override
    protected QueryResult executeWorkFlow(LogicalWorkflow logicalWorkflow)
            throws UnsupportedException, ExecutionException {
        QueryExecutor executor = new QueryExecutor(connectionHandler);

        throw new UnsupportedException("Not yet supported");

    }

    @Override
    public void asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
            throws ConnectorException {

        throw new UnsupportedException("Not yet supported");
    }

    @Override
    public void stop(String queryId) throws ConnectorException {

        throw new UnsupportedException("Not yet supported");
    }
}
