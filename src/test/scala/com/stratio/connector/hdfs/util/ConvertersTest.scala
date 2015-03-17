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

import com.stratio.crossdata.common.data.{Cell, Row}
import org.apache.spark.sql.{Row => SparkSQLRow}
import org.scalatest.{Matchers, FlatSpec}


class ConvertersTest extends FlatSpec
with Matchers{
  trait WithRowList {
    val list = List(1, 3, 5, 6)
    val xdRow = new Row ("List", new Cell(list))
    val sparkSqlRow = SparkSQLRow(list)
  }

  trait WithCell extends WithRowList{
    val cell = new Cell(new Cell(list))
    val sparkCell = SparkSQLRow(list)
  }

  behavior of "A CrossdataConverter"

  it should "convert from XDRow to SparkSQLRow" in new WithRowList {
    Converters.toSparkSQLRow(xdRow) should equal (sparkSqlRow)
  }

  it should "convert from Cell to SparkSQLRow" in new WithCell {
    Converters.extractCellValue(cell) should equal (sparkCell)
  }
}
