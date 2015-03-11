package com.stratio.connector.hdfs.scala.connection

import com.stratio.connector.commons.CommonsConnector
import com.stratio.connector.commons.connection.ConnectionHandler
import com.stratio.connector.commons.util.ManifestUtil
import com.stratio.connector.hdfs.engine.SqlEngine
import com.stratio.connector.hdfs.scala.engine.{StorageEngine, MetadataEngine}
import com.stratio.connector.hdfs.scala.engine.query.QueryEngine

import com.stratio.crossdata.common.connector._
import com.stratio.crossdata.connectors.ConnectorApp

class HDFSConnector extends CommonsConnector {
  
  import HDFSConnector._

  override def getConnectorName: String = ConnectorName

  override def getDatastoreName: Array[String] = DatastoreName

  override def init(configuration: IConfiguration): Unit = {
    val connectionHandler = new HDFSConnectionHandler(configuration)
  }

  override def getMetadataEngine: IMetadataEngine =
    new MetadataEngine (connectionHandler)

  override def getQueryEngine: IQueryEngine =
    new QueryEngine (connectionHandler)


  override def getSqlEngine: ISqlEngine =
    new SqlEngine (connectionHandler)

  override def getStorageEngine: IStorageEngine =
    new StorageEngine (connectionHandler)

}

object HDFSConnector extends App with ConnectorConstants{

  val HDFSConnector = new HDFSConnector

  val ConnectorApp = new ConnectorApp

  ConnectorApp.startup(HDFSConnector)
}

private[hdfs] trait ConnectorConstants {

  val ConnectorName = ManifestUtil.getConectorName("HDFSConnector.xml")

  val DatastoreName = ManifestUtil.getDatastoreName("HDFSConnector.xml")

  val MethodNotSupported: String = "Not supported yet"

}
