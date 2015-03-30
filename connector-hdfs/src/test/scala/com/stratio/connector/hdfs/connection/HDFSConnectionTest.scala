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

import java.util

import com.stratio.connector.hdfs.UnitSpec
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.data.ClusterName
import HDFSClient.defaultFileSystem

class HDFSConnectionTest extends UnitSpec {

   behavior of "An HDFS connection"

  it should "Close the connection when calling the close method" in {

    import scala.collection.JavaConversions._

    val clusterName = new ClusterName("ClusterName")


    val connectorOptions = new util.HashMap[String, String]()

    val clusterOptions: Map[String,String] = Map("hosts" -> "10.200.0.60:9000")

    val connectorClusterConfig = new ConnectorClusterConfig(clusterName, connectorOptions, clusterOptions)

    class FakeClient extends HDFSClient(defaultFileSystem(connectorClusterConfig),connectorClusterConfig)

    val hdfsClient = mock[FakeClient]

    val hdfsConnection = new HDFSConnection (hdfsClient, true)

    hdfsConnection.close()



    hdfsConnection.isConnected should equal (false)

  }
}
