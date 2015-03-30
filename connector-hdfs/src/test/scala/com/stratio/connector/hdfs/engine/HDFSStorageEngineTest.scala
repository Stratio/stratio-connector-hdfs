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

import com.stratio.connector.hdfs.UnitSpec
import com.stratio.crossdata.common.data.{Cell, Row}
import org.apache.spark.{SparkConf, SparkContext}

class HDFSStorageEngineTest extends UnitSpec{

  val sparkConf =  new SparkConf().setMaster("local[1]").setAppName("insert")

  class fakeSparkContext extends SparkContext(sparkConf)

  /*The Spark context*/
  val sparkContext = mock[fakeSparkContext]

  /*The SQL Context*/
  //val sqlContext = mock[fakeSparkContext]

  trait HDFSStorageEngineData {
    val hdfsStorageEng = new HDFSStorageEngine(connectionHandler, sparkContext)

  }

  behavior of "an HDFS Storage Engine"

  it should "insert one row in HDFS" in new HDFSStorageEngineData {

    private val cell= new Cell(1)
    val row = new Row("idTest", cell)

    hdfsStorageEng.insert(tableMetadata, row, isNotExists = false, hdfsConnection)

  }
}
