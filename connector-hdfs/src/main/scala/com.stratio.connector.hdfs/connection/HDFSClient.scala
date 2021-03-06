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

package com.stratio.connector.hdfs.connection

import com.stratio.connector.commons.{timer, Metrics, Loggable}
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import parquet.hadoop.metadata.CompressionCodecName
import timer._

/**
 * Class HDFSClient.
 *
 * @param hdfs The file system.
 * @param connectorClusterConfig The connector cluster configuration.
 * @param compressionCodec Compression codec set by default to SNAPPY.
 */
class HDFSClient(
  val hdfs: FileSystem,
  val connectorClusterConfig: ConnectorClusterConfig,
  val compressionCodec: CompressionCodecName = CompressionCodecName.SNAPPY)
  extends  Loggable with Metrics{

  private[hdfs] def createFolder (path: String): Unit = {

    val hdfsPath = new Path(s"$path")
    timeFor(s"Trying to create the folder $hdfsPath") {
      if (!hdfs.exists(hdfsPath))
        hdfs.mkdirs(hdfsPath)
      else logger.warn(s"The folder $hdfsPath is already created.")
    }
  }
}

object HDFSClient extends Loggable with Metrics{


  def apply(clusterConfig: ConnectorClusterConfig): HDFSClient =
    new HDFSClient(FileSystem.get(new Configuration()),clusterConfig)
}

