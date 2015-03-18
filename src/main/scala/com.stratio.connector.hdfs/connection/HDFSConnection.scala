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

import com.stratio.connector.commons.connection.Connection
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.security.ICredentials
import org.slf4j.LoggerFactory

/**
 * Class HDFS connection that creates the connection with HDFS.
 *
 * @param client The HDFS client.
 * @param isConnected True if it is connected, false in other case.
 */
class HDFSConnection(val client: HDFSClient, var isConnected: Boolean) extends Connection[HDFSClient] {

  /**
   * Closes the HDFS connection.
   */
  override def close(): Unit = {
    client.hdfs.close()
    isConnected = false
  }

  override def getNativeConnection: HDFSClient = client

}

object HDFSConnection {
  val logger = LoggerFactory.getLogger(getClass)

  def apply(config: ConnectorClusterConfig,
    credentials: Option[ICredentials] = None): HDFSConnection = {

    val connection = new HDFSConnection(HDFSClient(config), true)

    if(logger.isInfoEnabled){
      logger.info("New HDFS connection established")
    }
    connection
  }
}
