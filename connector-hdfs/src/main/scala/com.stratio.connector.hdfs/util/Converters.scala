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

package com.stratio.connector.hdfs.util

import com.stratio.crossdata.common.exceptions.UnsupportedException
import com.stratio.crossdata.common.metadata.{ColumnType, TableMetadata, DataType}
import com.stratio.crossdata.common.data.{Row => XDRow, Cell}

import org.apache.spark.sql.{Row => SparkSQLRow}

import org.apache.spark.sql.catalyst.expressions.GenericRow

import org.apache.spark.sql.types._

/**
 *  Object Converters.
 */
object Converters {

  import scala.collection.JavaConversions._

  /**
   * Method that extracts the value stored in a Cell from Crossdata
   * and load it in a Spark SQL row.
   * @param value
   * @return The Spark SQL row.
   */
  def extractCellValue(value: AnyRef): SparkSQLRow = {
    value match {
      case cell : Cell => extractCellValue(cell.getValue)
      case _ => SparkSQLRow(value)
    }
  }

  /**
   * Method that converts a Crossdata row into a Spark SQL row.
   * @param row
   * @return The Spark SQL row.
   */
  def toSparkSQLRow (row: XDRow): SparkSQLRow = {
    new GenericRow(row.getCellList.map { cell => cell.getValue match {
      case value: Cell => extractCellValue(value)
      case _ => cell.getValue
    }
    }.toArray[Any])
  }

  /**
   * Method that transforms the TableMetadata from Crossdata into a StructType
   * to be able to use it to generate the Spark SQL Schema.
   * @param tableMetadata
   * @return The TableMetadata in a StructType.
   */
  def toStructType(tableMetadata:TableMetadata):StructType ={
    val fields = tableMetadata.getColumns.toMap
    val structType = new StructType(
      fields.map{
        case(columnName, columnMetadata) =>
          new StructField(
            columnName.getName,
            columnMetadata.getColumnType.getDataType match{
              case DataType.BIGINT => LongType
              case DataType.BOOLEAN => BooleanType
              case DataType.DOUBLE => DoubleType
              case DataType.FLOAT => FloatType
              case DataType.INT => IntegerType
              case DataType.TEXT => StringType
              case DataType.VARCHAR => StringType
              case _ => throw new UnsupportedException("Type not supported")
            }
          )
      }.toArray
    )
    structType
  }
}
