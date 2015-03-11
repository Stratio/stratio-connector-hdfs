package com.stratio.connector.hdfs.engine

import com.stratio.connector.commons.connection.ConnectionHandler
import com.stratio.connector.commons.engine.CommonsSqlEngine
import com.stratio.connector.hdfs.scala.connection.HDFSConnector
import com.stratio.crossdata.common.connector.IResultHandler
import com.stratio.crossdata.common.exceptions.UnsupportedException
import com.stratio.crossdata.common.result.QueryResult

class SqlEngine(connectionHandler: ConnectionHandler)
  extends CommonsSqlEngine(connectionHandler) {
  override def pagedExecuteSQL(
    queryId: String,
    sqlQuery: String,
    resultHandler: IResultHandler,
    pageSize: Int): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)


  override def executeSQL(sqlQuery: String): QueryResult =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)


  override def asyncExecuteSQL(
    queryId: String,
    sqlQuery: String,
    resultHandler: IResultHandler): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

}
