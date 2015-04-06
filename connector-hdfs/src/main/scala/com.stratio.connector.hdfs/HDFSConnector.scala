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

import com.stratio.connector.commons.{Metrics, Loggable, timer, CommonsConnector}
import com.stratio.connector.commons.util.ManifestUtil
import com.stratio.connector.hdfs.connection.HDFSConnectionHandler
import com.stratio.connector.hdfs.engine.{HDFSMetadataEngine, HDFSStorageEngine}
import com.stratio.crossdata.common.connector._

import com.stratio.crossdata.common.exceptions.{ConnectionException, InitializationException}
import com.stratio.crossdata.common.security.ICredentials
import com.stratio.crossdata.common.exceptions.UnsupportedException

import com.stratio.crossdata.connectors.ConnectorApp
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try
import timer._

/**
 * Class HDFSConnector.
 */
class HDFSConnector extends CommonsConnector with Loggable with Metrics{

  import com.stratio.connector.hdfs.HDFSConnector._
  import com.stratio.connector.commons.util.PropertyValueRecovered

  var sparkContext:  Option[SparkContext] = None

  var metadataEngine: Option[HDFSMetadataEngine] = None

  var storageEngine: Option[HDFSStorageEngine] = None


  override def getConnectorName: String = ConnectorName

  override def getDatastoreName: Array[String] = DatastoreName

  /**
   * Method that creates the HDFSConnectionHandler.
   *
   * @param configuration The connector configuration.
   */
  override def init(configuration: IConfiguration): Unit = {
    connectionHandler =
      timeFor(s"Creating the HDFSConnectionHandler"){new HDFSConnectionHandler(configuration)}

  }

  /**
   * Method where the Spark Context is created and the 'hosts' property ir read.
   *
   * @param credentials The connector credentials.
   * @param config The connector configuration.
   */
  override def connect(

    credentials: ICredentials,
    config: ConnectorClusterConfig): Unit = {

    import scala.collection.JavaConversions._

    super.connect(credentials, config)

    /**
     * Creation of the Spark context.
     */
    sparkContext = Some({
      val sc = timeFor(s"Creating the SparkContext"){new SparkContext(
        new SparkConf().setMaster("local[1]").setAppName("insert"))}

      if (!PropertyValueRecovered.recoveredValue(classOf[Boolean],config.getClusterOptions.apply("highavailability"))){
        val SomeHostPort = config.getClusterOptions.toMap.get("hosts")

        if (!SomeHostPort.exists(_.length>0)){
          val message = "The host property is mandatory because highavailability property is set to false"
          logger.error(message)
          throw new ConnectionException(message)
        }

        val HostPort =
          timeFor(s"Recovering the host and the port required from the optional property 'hosts'"){PropertyValueRecovered.recoveredValue(classOf[String],SomeHostPort.get)}
        sc.hadoopConfiguration.set("fs.defaultFS",s"hdfs://$HostPort")
      }

      sc
    })


  }

  /**
   * Method that returns the metadataEngine.
   * @return The metadataEngine.
   */
  override def getMetadataEngine: IMetadataEngine = {
    synchronized {
      metadataEngine.getOrElse {

        logger.warn("Connector may not be initialized")
        metadataEngine = Option(new HDFSMetadataEngine(connectionHandler))
        metadataEngine.get
      }
    }
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
    synchronized {
      storageEngine.getOrElse {

        logger.warn("Connector may not be initialized")

        storageEngine = Option(new HDFSStorageEngine(
          connectionHandler,
          sparkContext.getOrElse(throw new InitializationException(s"The Spark" +
            s" context is not initialized"))))

        storageEngine.get
      }
    }
  }

  override def shutdown(): Unit ={

    super.shutdown()

    sparkContext.foreach(_.stop())
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


