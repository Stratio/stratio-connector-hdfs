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

import com.stratio.crossdata.common.data.{IndexName, TableName}
import com.stratio.crossdata.common.metadata.{IndexMetadata, ColumnMetadata, TableMetadata, CatalogMetadata}
import com.stratio.crossdata.common.statements.structures.Selector
import org.apache.hadoop.fs.Path
import com.stratio.crossdata.common.data.ColumnName
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, FlatSpec}

class HDFSMetadataEngineTest extends FlatSpec with Matchers with MockFactory with FSConstants{

  trait MetadataEngineData {

    val metadataEng = new HDFSMetadataEngine(connectionHandler)

    val options = Map[Selector, Selector]()

    val tables = Map[TableName, TableMetadata]()

    val indexes = Map[IndexName,IndexMetadata]()

    val columns = new util.LinkedHashMap[ColumnName, ColumnMetadata]()

    val partitionKey = List[ColumnName]()

    val clusterKey = List[ColumnName]()

    val pathCatalog = new Path(s"$catalogName")

    val pathTable = new Path(s"$catalogName/tablename")

  }

  import scala.collection.JavaConversions._

  behavior of "a Metadata Engine"

  it should "create a new catalog" in new MetadataEngineData {

    metadataEng.createCatalog(new CatalogMetadata(catalogName, options, tables),
      hdfsConnection)

    fakeFileSystem.exists(pathCatalog) should equal (true)

    fakeFileSystem.delete(pathCatalog, true)

    fakeFileSystem.close()

  }

  it should "create a new table" in new MetadataEngineData {

    metadataEng.createTable(new TableMetadata(tableName, options, columns, indexes, clusterName, partitionKey, clusterKey), hdfsConnection)

    fakeFileSystem.exists(pathTable) should equal (true)

    fakeFileSystem.delete(pathCatalog, true)

    fakeFileSystem.close()

  }
}
