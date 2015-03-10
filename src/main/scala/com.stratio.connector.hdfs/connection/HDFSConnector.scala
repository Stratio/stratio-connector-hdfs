package com.stratio.connector.hdfs.scala.connection

import com.stratio.connector.commons.CommonsConnector
import com.stratio.connector.commons.util.ManifestUtil

import com.stratio.crossdata.common.connector._
import com.stratio.crossdata.connectors.ConnectorApp

class HDFSConnector extends CommonsConnector {
  
  import HDFSConnector._

  override def getConnectorName: String = ConnectorName

  override def getDatastoreName: Array[String] = DatastoreName

  override def init(configuration: IConfiguration): Unit = ???

  override def getMetadataEngine: IMetadataEngine = ???

  override def getQueryEngine: IQueryEngine = ???

  override def getSqlEngine: ISqlEngine = ???

  override def getStorageEngine: IStorageEngine = ???

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
