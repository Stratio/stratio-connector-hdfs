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

import com.stratio.connector.commons.connection.{Connection, ConnectionHandler}
import com.stratio.connector.hdfs.connection.HDFSClient
import com.stratio.crossdata.common.metadata.CatalogMetadata
import org.junit.{Test, Before}
import org.mockito.{Matchers, Mock}
import org.mockito.Mockito.when

abstract class MetEngTest {

  @Mock
  val connectionHandler:ConnectionHandler

  @Mock
  val connection:Connection[HDFSClient]

  @Mock
  val client:HDFSClient

  val CLUSTER_NAME: String = "Cluster_Name"
//
//  @Before
//  def before(): Unit ={
//    when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection)
//    when(connection.getNativeConnection).thenReturn(client)
//  }
//
//  @Test
//  def createCatalogTest(): Unit ={
//    val metadataEngine = new MetadataEngine(connectionHandler)
//    metadataEngine.createCatalog(Matchers.any(), connection)
//  }
}
