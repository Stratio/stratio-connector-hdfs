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

import com.stratio.connector.commons.{Metrics, Loggable}
import com.stratio.connector.commons.connection.{ConnectionHandler, Connection}
import com.stratio.connector.commons.engine.CommonsMetadataEngine
import com.stratio.connector.hdfs.HDFSConnector
import com.stratio.connector.hdfs.connection.{HDFSConnection, HDFSClient}
import com.stratio.crossdata.common.data.{ClusterName, CatalogName,
AlterOptions, TableName}
import com.stratio.crossdata.common.exceptions.{ExecutionException, UnsupportedException}
import com.stratio.crossdata.common.metadata.{TableMetadata, IndexMetadata,
CatalogMetadata}
import com.stratio.crossdata.common.statements.structures.Selector

/**
 * Class MetadataEngine.
 *
 * @param connectionHandler The connection handler that contains
 *                          the configuration.
 */
class HDFSMetadataEngine(connectionHandler: ConnectionHandler)
  extends CommonsMetadataEngine[HDFSClient](connectionHandler) with Loggable with Metrics{

  override def provideMetadata(
                                targetCluster: ClusterName,
                                connection: Connection[HDFSClient]): util.List[CatalogMetadata] =
    throw new UnsupportedException(s"Method provideMetadata is ${HDFSConnector.MethodNotSupported}")

  override def provideTableMetadata(
                                     tableName: TableName,
                                     targetCluster: ClusterName,
                                     connection: Connection[HDFSClient]): TableMetadata =
    throw new UnsupportedException(s"Method provideTableMetadata is ${HDFSConnector.MethodNotSupported}")

  override def provideCatalogMetadata(
                                       catalogName: CatalogName,
                                       targetCluster: ClusterName,
                                       connection: Connection[HDFSClient]): CatalogMetadata =
    throw new UnsupportedException(s"Method provideCatalogMetadata is ${HDFSConnector.MethodNotSupported}")

  /**
   * Operation that creates a directory in representation of the table.
   *
   * @param tableMetadata Metadata from the table to be created.
   * @param connection The HDFS connection.
   */
  override def createTable(
                            tableMetadata: TableMetadata,
                            connection: Connection[HDFSClient]): Unit = {
    logger.info("The Create table method has been invoked. This method doesn't do anything the table will be created when the first insert happens.")
  }


  override def createIndex(
    indexMetadata: IndexMetadata,
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(s"Method createIndex is ${HDFSConnector.MethodNotSupported}")

  override def dropCatalog(
    name: CatalogName,
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(s"Method dropCatalog is ${HDFSConnector.MethodNotSupported}")

  override def alterCatalog(
    catalogName: CatalogName,
    options: util.Map[Selector, Selector],
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(s"Method alterCatalog is ${HDFSConnector.MethodNotSupported}")

  override def alterTable(
    name: TableName,
    alterOptions: AlterOptions,
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(s"Method alterTable is ${HDFSConnector.MethodNotSupported}")

  override def dropTable(
    name: TableName,
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(s"Method dropTable is ${HDFSConnector.MethodNotSupported}")

  override def dropIndex(
    indexMetadata: IndexMetadata,
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(s"Method dropIndex is ${HDFSConnector.MethodNotSupported}")

  /**
   * Operation that creates a directory in representation of the catalog.
   *
   * @param catalogMetadata Metadata from the catalog to be created.
   * @param connection The HDFS connection.
   */
  override def createCatalog(
    catalogMetadata: CatalogMetadata,
    connection: Connection[HDFSClient]): Unit = {

    logger.info("The Create catalog method has been invoked. This method doesn't do anything the table will be created when the first insert happens.")

  }
}
