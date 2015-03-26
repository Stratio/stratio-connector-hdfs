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

package com.stratio.connector.hdfs

import com.stratio.connector.commons.CommonsConnector
import com.stratio.connector.commons.util.ManifestUtil
import com.stratio.connector.hdfs.connection.HDFSConnectionHandler
import com.stratio.connector.hdfs.engine.{HDFSMetadataEngine, HDFSStorageEngine}
import com.stratio.crossdata.common.connector._
import com.stratio.crossdata.common.exceptions.UnsupportedException
import com.stratio.crossdata.connectors.ConnectorApp
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.Try

class HDFSConnector extends CommonsConnector {

  import com.stratio.connector.hdfs.HDFSConnector._

  /**
   * Creation of the Spark context.
   */
  private lazy val sparkContext: SparkContext = new SparkContext(
    new SparkConf().setMaster("local[1]").setAppName("insert"))

  implicit val logger = LoggerFactory.getLogger(getClass)

  lazy val metadataEngine: HDFSMetadataEngine = new HDFSMetadataEngine(connectionHandler)

  lazy val storageEngine:HDFSStorageEngine = new HDFSStorageEngine(
    connectionHandler,
    sparkContext)

  override def getConnectorName: String = ConnectorName

  override def getDatastoreName: Array[String] = DatastoreName

  override def init(configuration: IConfiguration): Unit = {
    connectionHandler = new HDFSConnectionHandler(configuration)

  }

  /**
   * Return the metadataEngine.
   * @return The metadataEngine.
   */
  override def getMetadataEngine: IMetadataEngine = {
    metadataEngine
  }

  /**
   * Return the queryEngine.
   * @return UnsupportedException.
   * @throws UnsupportedException the operation is not supported.
   */
  override def getQueryEngine: IQueryEngine = {

    throw new UnsupportedException (s"Query Engine is $MethodNotSupported")
  }

  /**
   * Return the StorageEngine.
   * @return The storageEngine.
   */
  override def getStorageEngine: IStorageEngine = {
      storageEngine
  }

  override def shutdown(): Unit ={

    super.shutdown()

    sparkContext.stop()
  }
  /**
   * Run the shutdown.
   */
  def attachShutDownHook(): Unit = {

    Runtime.getRuntime.addShutdownHook(new Thread() {

      override def run(): Unit = {
        Try {
          shutdown()
        }.recover {
          case e => logger.error("Fail ShutDown")
        }
      }
    })
  }

}

/**
 * Launch the connector.
 */
object HDFSConnector extends App with ConnectorConstants{

  val HDFSConnector = new HDFSConnector

  new ConnectorApp().startup(HDFSConnector)

  HDFSConnector.attachShutDownHook()

}

private[hdfs] trait ConnectorConstants {

  val ConnectorName = ManifestUtil.getConectorName("HDFSConnector.xml")

  val DatastoreName = ManifestUtil.getDatastoreName("HDFSConnector.xml")

  val MethodNotSupported: String = "not supported yet"




}


