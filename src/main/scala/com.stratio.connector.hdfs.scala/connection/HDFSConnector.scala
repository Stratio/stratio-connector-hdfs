package com.stratio.connector.hdfs.scala.connection

import com.stratio.crossdata.common.connector._
import com.stratio.crossdata.common.data.ClusterName
import com.stratio.crossdata.common.security.ICredentials
import com.stratio.crossdata.connectors.ConnectorApp

class HDFSConnector extends IConnector {
  override def getConnectorName: String = ???

  override def getDatastoreName: Array[String] = ???

  override def shutdown(): Unit = ???

  override def init(configuration: IConfiguration): Unit = ???

  override def getMetadataEngine: IMetadataEngine = ???

  override def getQueryEngine: IQueryEngine = ???

  override def isConnected(name: ClusterName): Boolean = ???

  override def close(name: ClusterName): Unit = ???

  override def getSqlEngine: ISqlEngine = ???

  override def connect(credentials: ICredentials, config: ConnectorClusterConfig): Unit = ???

  override def getStorageEngine: IStorageEngine = ???
}

object HDFSConnector extends App with Constants {

  val hdfsConnector = new HDFSConnector

  val connectorApp = new ConnectorApp

  connectorApp.startup(hdfsConnector)
}


private[hdfs] trait Constants {
  val MethodNotSupported: String = "Not supported yet"
}