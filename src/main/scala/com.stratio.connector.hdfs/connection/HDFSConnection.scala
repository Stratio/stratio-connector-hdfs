package com.stratio.connector.hdfs.scala.connection

import com.stratio.connector.commons.connection.Connection
import com.stratio.connector.hdfs.scala.HDFSClient
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.security.ICredentials
import org.slf4j.LoggerFactory


class HDFSConnection(val client: HDFSClient, var isConnected: Boolean) extends Connection[HDFSClient] {

  override def close(): Unit = {
    client.hdfs.close
    isConnected = false
  }

  override def getNativeConnection: HDFSClient = client

}

object HDFSConnection {
  val logger = LoggerFactory.getLogger(getClass)

  def apply(config: ConnectorClusterConfig,
    credentials: Option[ICredentials] = None): HDFSConnection = {

    val connection = new HDFSConnection(HDFSClient(config), true)

    if(logger.isInfoEnabled()){
      logger.info("New HDFS connection established");
    }
    connection
  }
}
