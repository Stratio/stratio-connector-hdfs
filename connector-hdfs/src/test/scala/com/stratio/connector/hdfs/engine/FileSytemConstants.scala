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
import com.stratio.crossdata.common.data._
import com.stratio.crossdata.common.metadata.{TableMetadata, IndexMetadata, ColumnMetadata}
import com.stratio.crossdata.common.statements.structures.Selector
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapred.JobConf

trait FileSytemConstants{

  /*Connector configuration.*/
  val configuration = new IConfiguration {}

  /*The connection handler.*/
  val connectionHandler = new HDFSConnectionHandler(configuration)

  /*The table name.*/
  val catalogName = new CatalogName("CatalogName")

  /*The catalog name.*/
  val tableName = new TableName("CatalogName", "TableName")

  /*The tables of the catalog.*/
  val tables = Map[TableName, TableMetadata]()

  /*The cluster name.*/
  val clusterName = new ClusterName("ClusterName")

  /*The connector options.*/
  val connectorOptions = new util.HashMap[String, String]()

  /*The cluster options.*/
  val clusterOptions: Map[String,String] =
    Map("hosts" -> "10.200.0.60:9000", "user" -> "lfernandez", "path" -> "", "highavailability" -> "false")

  import scala.collection.JavaConversions._
  /*Configuration used by a connector to establish a connection to a specific cluster.*/
  val connectorClusterConfig =
    new ConnectorClusterConfig(clusterName, connectorOptions, clusterOptions)

  /*The HDFS for testing*/
  val fakeFileSystem = FileSystem.get(new JobConf())

  /*THe HDFS client*/
  val hDFSClient = new HDFSClient(fakeFileSystem, connectorClusterConfig)

  /*The HDFS connection*/
  val hdfsConnection = new HDFSConnection (hDFSClient, true)

  /*Options of the table to be created.*/
  val options = Map[Selector, Selector]()

  /*The list of columns metadata of the table.*/
  val columns = new util.LinkedHashMap[ColumnName, ColumnMetadata]()

  /*The list of indexes of the columns of the table.*/
  val indexes = Map[IndexName,IndexMetadata]()

  /*The list of columns that conforms the partition key.*/
  val partitionKey = List[ColumnName]()

  /*The list of columns tha conforms the cluster key.*/
  val clusterKey = List[ColumnName]()

  /*The table metadata*/
  val tableMetadata =
    new TableMetadata(tableName, options, columns, indexes, clusterName, partitionKey, clusterKey)

  /*The path to the created table*/
  val pathTable = new Path(s"$catalogName/tablename")


}
