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

import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.data.ClusterName
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, FlatSpec}
import java.util.HashMap

import parquet.hadoop.metadata.CompressionCodecName

class HDFSConnectionTest extends FlatSpec with Matchers with MockFactory {

  trait ConnectionData {
    val isConnected = false
  }

 /* behavior of "An HDFS connection"

  it should "Close the connection when calling the close method" in new ConnectionData{
    val clusterName = new ClusterName("ClusterName")

    val connectorOptions = new HashMap[String, String]()
    val clusterOptions = new HashMap[String, String]()

    val connectorClusterConfig = new ConnectorClusterConfig(clusterName, connectorOptions, clusterOptions)

    var hdfsClient: HDFSClient = new HDFSClient(null, connectorClusterConfig, CompressionCodecName.SNAPPY)

    val hdfsConnection = new HDFSConnection (hdfsClient, true)
    hdfsConnection.close()
    hdfsConnection.isConnected should equal (isConnected)
  }*/
}
