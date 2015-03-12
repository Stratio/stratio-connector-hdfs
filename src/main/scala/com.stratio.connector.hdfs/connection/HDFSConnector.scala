package com.stratio.connector.hdfs.scala.connection

import com.stratio.connector.commons.CommonsConnector
import com.stratio.connector.commons.connection.ConnectionHandler
import com.stratio.connector.commons.util.ManifestUtil
import com.stratio.connector.hdfs.engine.SqlEngine
import com.stratio.connector.hdfs.scala.engine.{StorageEngine, MetadataEngine}
import com.stratio.connector.hdfs.scala.engine.query.QueryEngine

import com.stratio.crossdata.common.connector._
import com.stratio.crossdata.connectors.ConnectorApp
import org.slf4j.{LoggerFactory, Logger}

class HDFSConnector extends CommonsConnector {

  import HDFSConnector._

  implicit val logger = LoggerFactory.getLogger(getClass)

  var metadataEngine: Option[MetadataEngine] = None

  var queryEngine: Option[QueryEngine] = None

  var storageEngine: Option[StorageEngine] = None

  var sqlEngine: Option[SqlEngine] = None

  override def getConnectorName: String = ConnectorName

  override def getDatastoreName: Array[String] = DatastoreName

  override def init(configuration: IConfiguration): Unit = {
    connectionHandler = new HDFSConnectionHandler(configuration)
    metadataEngine =  Some(new MetadataEngine (connectionHandler))
    queryEngine = Some(new QueryEngine (connectionHandler))
    storageEngine = Some(new StorageEngine (connectionHandler))
    sqlEngine = Some(new SqlEngine(connectionHandler))
  }
  override def getMetadataEngine: IMetadataEngine = {
    metadataEngine.getOrElse{
      logger.warn("Connector may not be initialized")
      new MetadataEngine (connectionHandler)
    }
  }

  //TODO: Falta hacer lo mismo que para el MetadataEngine
  override def getQueryEngine: IQueryEngine = {
    queryEngine.getOrElse {
      logger.warn("Connector may not be initialized")
      new QueryEngine(connectionHandler)
    }
  }

  override def getSqlEngine: ISqlEngine =
    new SqlEngine (connectionHandler)

  override def getStorageEngine: IStorageEngine = {
    storageEngine.getOrElse {
      logger.warn("Connector may not be initialized")
      new StorageEngine(connectionHandler)
    }
  }
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
