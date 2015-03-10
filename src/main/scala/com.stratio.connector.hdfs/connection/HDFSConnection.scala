package com.stratio.connector.hdfs.scala.connection

import com.stratio.connector.commons.connection.Connection
import com.stratio.connector.hdfs.scala.HDFSClient
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.security.ICredentials
import org.slf4j.LoggerFactory


class HDFSConnection(val client: HDFSClient) extends Connection[HDFSClient] {

  override def close(): Unit = client.hdfs.map(_.close)

  override def getNativeConnection: HDFSClient = client

  override def isConnected: Boolean = client.hdfs.isDefined
}

object HDFSConnection {
  val logger = LoggerFactory.getLogger(getClass)

  def apply(config: ConnectorClusterConfig,
    credentials: Option[ICredentials] = None): HDFSConnection = {
    val connection = new HDFSConnection(HDFSClient(config))
    if(logger.isInfoEnabled()){
      logger.info("New HDFS connection established");
    }
    connection
  }
}
