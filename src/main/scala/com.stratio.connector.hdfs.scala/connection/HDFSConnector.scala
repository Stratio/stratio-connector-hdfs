package com.stratio.connector.hdfs.scala.connection

import com.stratio.connector.commons.CommonsConnector
import com.stratio.connector.commons.util.ManifestUtil
import com.stratio.connector.hdfs.configuration.HDFSConstants
import com.stratio.connector.hdfs.scala.configuration.Constants
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration

import com.stratio.crossdata.common.connector._
import com.stratio.crossdata.common.data.ClusterName
import com.stratio.crossdata.common.security.ICredentials
import com.stratio.crossdata.connectors.ConnectorApp
import org.slf4j.LoggerFactory


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
}