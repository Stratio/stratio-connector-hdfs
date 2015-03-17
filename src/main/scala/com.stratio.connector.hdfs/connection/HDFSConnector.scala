/*
 *
 *  Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.connector.hdfs.connection

import com.stratio.connector.commons.CommonsConnector
import com.stratio.connector.commons.util.ManifestUtil
import com.stratio.connector.hdfs.engine.{StorageEngine, MetadataEngine}

import com.stratio.crossdata.common.connector._
import com.stratio.crossdata.common.exceptions.UnsupportedException
import com.stratio.crossdata.connectors.ConnectorApp
import org.slf4j.LoggerFactory

class HDFSConnector extends CommonsConnector {

  import HDFSConnector._

  implicit val logger = LoggerFactory.getLogger(getClass)

  var metadataEngine: Option[MetadataEngine] = None

  var storageEngine: Option[StorageEngine] = None

  override def getConnectorName: String = ConnectorName

  override def getDatastoreName: Array[String] = DatastoreName

  override def init(configuration: IConfiguration): Unit = {
    connectionHandler = new HDFSConnectionHandler(configuration)
    metadataEngine =  Some(new MetadataEngine (connectionHandler))
    storageEngine = Some(new StorageEngine (connectionHandler))
  }

  override def getMetadataEngine: IMetadataEngine = {
    metadataEngine.getOrElse{
      logger.warn("Connector may not be initialized")
      new MetadataEngine (connectionHandler)
    }
  }

  override def getQueryEngine: IQueryEngine = {
    throw new UnsupportedException (MethodNotSupported)
  }

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

  val HostPort: String = "conectores3:9000"

}
