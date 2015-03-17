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
import com.stratio.connector.commons.engine.SingleProjectQueryEngine
import com.stratio.connector.hdfs.connection.HDFSConnector
import com.stratio.crossdata.common.connector.IResultHandler
import com.stratio.crossdata.common.exceptions.UnsupportedException
import com.stratio.crossdata.common.logicalplan.{Project, LogicalWorkflow}
import com.stratio.crossdata.common.result.QueryResult

class SingleProjectQEngine (connectionHandler: ConnectionHandler)
  extends SingleProjectQueryEngine(connectionHandler){

  override def execute(
    workflow: Project,
    connection: Connection[Nothing]): QueryResult =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def pagedExecuteWorkFlow(
    queryId: String,
    workflow: LogicalWorkflow,
    resultHandler: IResultHandler,
    pageSize: Int): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def asyncExecuteWorkFlow(
    queryId: String,
    workflow: LogicalWorkflow,
    resultHandler: IResultHandler): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)

  override def stop(queryId: String): Unit =
    throw new UnsupportedException(HDFSConnector.MethodNotSupported)
}
