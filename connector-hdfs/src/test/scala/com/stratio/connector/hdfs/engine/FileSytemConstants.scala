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

package com.stratio.connector.hdfs.engine

import java.util

import com.stratio.connector.hdfs.connection.{HDFSConnection, HDFSClient, HDFSConnectionHandler}
import com.stratio.crossdata.common.connector.{ConnectorClusterConfig, IConfiguration}
import com.stratio.crossdata.common.data.{ClusterName, CatalogName}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.JobConf

trait FSConstants{

  val configuration = new IConfiguration {}

  val connectionHandler = new HDFSConnectionHandler(configuration)

  val catalogName = new CatalogName("CatalogName")

  val clusterName = new ClusterName("ClusterName")

  val connectorOptions = new util.HashMap[String, String]()

  val clusterOptions: Map[String,String] = Map("hosts" -> "10.200.0.60:9000")

  import scala.collection.JavaConversions._
  val connectorClusterConfig = new ConnectorClusterConfig(clusterName, connectorOptions, clusterOptions)

  val FakeFileSystem = FileSystem.get(new JobConf())

  val hDFSClient = new HDFSClient(FakeFileSystem, connectorClusterConfig)

  val hdfsConnection = new HDFSConnection (hDFSClient, true)

}
