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

import com.stratio.connector.hdfs.HDFSClient
import com.stratio.connector.hdfs.connection.HDFSConnection
import com.stratio.connector.hdfs.util.Converters
import com.stratio.connector.hdfs.connection.HDFSConnector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.{sql, SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

import com.stratio.connector.commons.engine.CommonsStorageEngine
import com.stratio.crossdata.common.data.{Row, TableName}
import com.stratio.crossdata.common.exceptions.{ExecutionException, UnsupportedException}
import com.stratio.crossdata.common.logicalplan.Filter
import com.stratio.crossdata.common.metadata.TableMetadata
import com.stratio.crossdata.common.statements.structures.Relation
import com.stratio.connector.commons.connection.{Connection, ConnectionHandler}

/**
 * Class StorageEngine.
 *
 * @param connectionHandler The connection handler that contains
 *                          the configuration.
 */
class StorageEngine(connectionHandler: ConnectionHandler)
  extends CommonsStorageEngine[HDFSClient](connectionHandler) {

  /**
   * Creation of the Spark context.
   */
  lazy val sparkContext:  SparkContext ={
    val sc = new SparkContext(
      new SparkConf().setMaster("local[1]").setAppName("insert"))
    sc.hadoopConfiguration.set("fs.defaultFS",s"hdfs://$HostPort")
    sc
  }

  /**
   * The logger.
   */
  implicit val logger = LoggerFactory.getLogger(getClass)

  override def truncate(
    tableName: TableName,
    connection: Connection[HDFSClient]): Unit =

    throw new UnsupportedException(MethodNotSupported)

  override def update(
    tableName: TableName,
    assignments: util.Collection[Relation],
    whereClauses: util.Collection[Filter],
    connection: Connection[HDFSClient]): Unit =

    throw new UnsupportedException(MethodNotSupported)

  /**
   * Insert one row in HDFS with parquet format.
   *
   * @param targetTable Metadata from the table where insertion is expected.
   * @param row Row of information to be inserted.
   * @param isNotExists isNotExists.
   * @param connection The HDFS connection.
   */
  override def insert(
    targetTable: TableMetadata,
    row: Row,
    isNotExists: Boolean,
    connection: Connection[HDFSClient]): Unit = {

    insert(targetTable, List(row), isNotExists, connection)

  }

  /**
   * Insert a collection of rows in HDFS with parquet format.
   *
   * @param targetTable Metadata from the table where insertion is expected.
   * @param rows Collection of rows of information to be inserted.
   * @param isNotExists isNotExists.
   * @param connection The HDFS connection.
   */
  override def insert(
    targetTable: TableMetadata,
    rows: util.Collection[Row],
    isNotExists: Boolean,
    connection: Connection[HDFSClient]): Unit = {

    if (isNotExists)
      throw new UnsupportedException(MethodNotSupported)

    val catalog = targetTable.getName.getCatalogName.getName

    val tableName = targetTable.getName.getName

    val sqlContext = new SQLContext(sparkContext)

    import scala.collection.JavaConversions._

    val user =   connection match {
      case c: HDFSConnection =>
        c.client.connectorClusterConfig.getClusterOptions.apply("User")
      case _ => throw new ExecutionException(
        s"The given connection $connection is not an HDFS connection")
    }

    val path = s"/user/$user/$catalog/$tableName"
    val rdd: RDD[sql.Row] = sqlContext.sparkContext.parallelize(rows.toSeq,1)
      .map(row => Converters.toSparkSQLRow(row))

    val schema: StructType = Converters.toStructType(targetTable)

    val dataFrame: DataFrame = sqlContext.createDataFrame(rdd, schema)

    dataFrame.save(path,"parquet",SaveMode.Append)

  }

  override def delete(
    tableName: TableName,
    whereClauses: util.Collection[Filter],
    connection: Connection[HDFSClient]): Unit =
    throw new UnsupportedException(MethodNotSupported)
}
