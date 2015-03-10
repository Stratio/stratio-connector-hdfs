package com.stratio.connector.hdfs.scala.engine

import java.util


import com.stratio.connector.hdfs.scala.HDFSClient
import org.apache.hadoop.fs.Path
import parquet.hadoop.ParquetOutputFormat

import scala.collection.JavaConversions._

import com.stratio.connector.commons.connection.Connection
import com.stratio.connector.commons.engine.CommonsStorageEngine
import com.stratio.connector.hdfs.scala.connection.{HDFSConnector, HDFSConnectionHandler}
import com.stratio.crossdata.common.data.{Row, TableName}
import com.stratio.crossdata.common.exceptions.{ExecutionException, UnsupportedException}
import com.stratio.crossdata.common.logicalplan.Filter
import com.stratio.crossdata.common.metadata.TableMetadata
import com.stratio.crossdata.common.statements.structures.Relation

import scala.util._


class StorageEngine(connectionHandler: HDFSConnectionHandler)
  extends CommonsStorageEngine[HDFSClient](connectionHandler) {

  implicit class FinallyHelper[T](t: scala.util.Try[T]){
    def butFinally[U](f: => U) =
      if (t.isSuccess) t.map(_ => f) else t.recover{case t:Throwable => f; throw t}
  }


  override def truncate(
    tableName: TableName,
    connection: Connection[HDFSClient]): Unit =

    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def update(
    tableName: TableName,
    assignments: util.Collection[Relation],
    whereClauses: util.Collection[Filter],
    connection: Connection[HDFSClient]): Unit =

    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def insert(
    targetTable: TableMetadata,
    row: Row, isNotExists: Boolean,
    connection: Connection[HDFSClient]): Unit = {

    insert(targetTable, List(row), isNotExists, connection)

  }

  override def insert(
    targetTable: TableMetadata,
    rows: util.Collection[Row],
    isNotExists: Boolean,
    connection: Connection[HDFSClient]): Unit = {

    if (isNotExists)
      throw new UnsupportedException(HDFSConnector.MethodNotSupported)

    val hdfsClient: HDFSClient = connection.getNativeConnection
    val catalog = targetTable.getName.getCatalogName.getName
    val tableName = targetTable.getName.getName

    val linkedColumns =
      hdfsClient.getMetaDataInfoFromFile(s"$catalog/$tableName")

    val format = new ParquetOutputFormat[Row]
    val path = new Path(s"$catalog/$tableName")
    val codec = hdfsClient.compressionCodec
    val hadoopConf= hdfsClient.hdfs.get.getConf

    val writer = format.getRecordWriter(hadoopConf, path, codec)
    Try {
      while (rows.iterator().hasNext) {
        val row = rows.iterator().next()
        writer.write(null, row)
      }
    }.transform(s => {
      writer.close(null)
      Success(s)
    }, f => {
      writer.close(null)
      throw new ExecutionException(s"Impossible to write in the path $path")
      Failure(f)
    })
  }

  override def delete(
    tableName: TableName,
    whereClauses: util.Collection[Filter],
    connection: Connection[HDFSClient]): Unit = ???
}
