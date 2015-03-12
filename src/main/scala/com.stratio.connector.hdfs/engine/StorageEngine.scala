package com.stratio.connector.hdfs.scala.engine

import java.util


import com.stratio.connector.commons.timer
import com.stratio.connector.hdfs.scala.HDFSClient
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory
import parquet.hadoop.ParquetOutputFormat

import scala.collection.JavaConversions._


import com.stratio.connector.commons.engine.CommonsStorageEngine
import com.stratio.connector.hdfs.scala.connection.HDFSConnector
import com.stratio.crossdata.common.data.{Row, TableName}
import com.stratio.crossdata.common.exceptions.{ExecutionException, UnsupportedException}
import com.stratio.crossdata.common.logicalplan.Filter
import com.stratio.crossdata.common.metadata.TableMetadata
import com.stratio.crossdata.common.statements.structures.Relation
import com.stratio.connector.commons.connection.{Connection, ConnectionHandler}

import scala.util._


class StorageEngine(connectionHandler: ConnectionHandler)
  extends CommonsStorageEngine[HDFSClient](connectionHandler) {

  implicit class FinallyHelper[T](t: scala.util.Try[T]){
    def butFinally[U](f: => U) =
      if (t.isSuccess) t.map(_ => f)
      else t.recover{case t:Throwable => f; throw t}
  }

  implicit val logger = LoggerFactory.getLogger(getClass)

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
    row: Row,
    isNotExists: Boolean,
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


    val sqlContext = new SQLContext(new SparkContext(
      new SparkConf().setMaster("local[1]").setAppName("insert")))

    val format = new ParquetOutputFormat[Row]

    val hadoopConf= hdfsClient.hdfs.getConf

    val path = new Path(s"$catalog/$tableName")

    val codec = hdfsClient.compressionCodec

    val writer = format.getRecordWriter(hadoopConf, path, codec)

    // TODO: Poner bien lo del timer
    Try {
      while (rows.iterator().hasNext) {
        val row = rows.iterator().next()
        import timer._
        //time("mensaje opcional"){bloque de código}
        time (s"Writing $row with $writer"){
          writer.write(null, row)
        }
      }
    }.transform(s => {
      logger.debug("Before invoking the method close in the success")
      writer.close(null)
      logger.debug("After invoking the method close in the success")

      Success(s)
    }, f => {
      logger.debug("Before invoking the method close in the failure")
      writer.close(null)
      logger.debug("After invoking the method close in the failure")

      throw new ExecutionException(s"Impossible to write in the path $path")
      Failure(f)
    })
  }

  override def delete(
    tableName: TableName,
    whereClauses: util.Collection[Filter],
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)
}
