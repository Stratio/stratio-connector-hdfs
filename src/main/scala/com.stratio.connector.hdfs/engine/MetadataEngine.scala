package com.stratio.connector.hdfs.scala.engine

import java.util

import com.stratio.connector.commons.connection.{ConnectionHandler, Connection}
import com.stratio.connector.commons.engine.CommonsMetadataEngine
import com.stratio.connector.hdfs.scala.HDFSClient
import com.stratio.connector.hdfs.scala.connection.HDFSConnector
import com.stratio.crossdata.common.data.{
ClusterName,
CatalogName,
AlterOptions,
TableName}
import com.stratio.crossdata.common.exceptions.UnsupportedException
import com.stratio.crossdata.common.metadata.{
TableMetadata,
IndexMetadata,
CatalogMetadata}
import com.stratio.crossdata.common.statements.structures.Selector

class MetadataEngine(connectionHandler: ConnectionHandler)
  extends CommonsMetadataEngine[HDFSClient](connectionHandler) {

  override def provideMetadata(
    targetCluster: ClusterName,
    connection: Connection[HDFSClient]): util.List[CatalogMetadata] =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def provideTableMetadata(
    tableName: TableName,
    targetCluster: ClusterName,
    connection: Connection[HDFSClient]): TableMetadata =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def provideCatalogMetadata(
    catalogName: CatalogName,
    targetCluster: ClusterName,
    connection: Connection[HDFSClient]): CatalogMetadata =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def createTable(
    tableMetadata: TableMetadata,
    connection: Connection[HDFSClient]): Unit =
    connection.getNativeConnection.createFolder(
      s"${tableMetadata.getName.getCatalogName.getName}" +
        s"/${tableMetadata.getName.getName}")

  override def createIndex(
    indexMetadata: IndexMetadata,
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def dropCatalog(
    name: CatalogName,
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def alterCatalog(
    catalogName: CatalogName,
    options: util.Map[Selector, Selector],
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def alterTable(
    name: TableName,
    alterOptions: AlterOptions,
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def dropTable(
    name: TableName,
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def dropIndex(
    indexMetadata: IndexMetadata,
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def createCatalog(
    catalogMetadata: CatalogMetadata,
    connection: Connection[HDFSClient]): Unit = {

    connection.getNativeConnection.createFolder(
      s"${catalogMetadata.getName.getName}")
  }
}
