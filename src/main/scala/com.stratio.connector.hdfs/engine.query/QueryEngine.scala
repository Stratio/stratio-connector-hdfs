package com.stratio.connector.hdfs.engine.query

import com.stratio.crossdata.common.connector.{IResultHandler, IQueryEngine, ConnectorClusterConfig}
import com.stratio.crossdata.common.exceptions.UnsupportedException
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow
import com.stratio.crossdata.common.result.QueryResult


case class QueryEngine(
config: ConnectorClusterConfig) extends IQueryEngine {
  override def execute(workflow: LogicalWorkflow): QueryResult =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def stop(queryId: String): Unit = ???

  override def pagedExecute(queryId: String, workflow: LogicalWorkflow, resultHandler: IResultHandler, pageSize: Int): Unit = ???

  override def asyncExecute(queryId: String, workflow: LogicalWorkflow, resultHandler: IResultHandler): Unit = ???
}
