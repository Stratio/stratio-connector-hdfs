package com.stratio.connector.hdfs.engine

import com.stratio.connector.commons.connection.{Connection, ConnectionHandler}
import com.stratio.connector.commons.engine.SingleProjectQueryEngine
import com.stratio.connector.hdfs.scala.connection.HDFSConnector
import com.stratio.crossdata.common.connector.IResultHandler
import com.stratio.crossdata.common.exceptions.UnsupportedException
import com.stratio.crossdata.common.logicalplan.{Project, LogicalWorkflow}
import com.stratio.crossdata.common.result.QueryResult

class SingleProjectQEngine (connectionHandler: ConnectionHandler)
  extends SingleProjectQueryEngine(connectionHandler){

  override def execute(
    workflow: Project,
    connection: Connection[Nothing]): QueryResult =
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
