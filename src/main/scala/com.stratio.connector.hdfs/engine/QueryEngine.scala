package com.stratio.connector.hdfs.scala.engine.query

import com.stratio.connector.commons.engine.CommonsQueryEngine
import com.stratio.connector.hdfs.scala.connection.{
HDFSConnector,
HDFSConnectionHandler}
import com.stratio.crossdata.common.connector.IResultHandler
import com.stratio.crossdata.common.exceptions.UnsupportedException
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow
import com.stratio.crossdata.common.result.QueryResult


class QueryEngine(hdfsConnectionHandler: HDFSConnectionHandler)
  extends CommonsQueryEngine(hdfsConnectionHandler) {

  override def executeWorkFlow(workflow: LogicalWorkflow): QueryResult =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def pagedExecuteWorkFlow(
    queryId: String,
    workflow: LogicalWorkflow,
    resultHandler: IResultHandler,
    pageSize: Int): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def asyncExecuteWorkFlow(
    queryId: String,
    workflow: LogicalWorkflow,
    resultHandler: IResultHandler): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def stop(queryId: String): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)
}

