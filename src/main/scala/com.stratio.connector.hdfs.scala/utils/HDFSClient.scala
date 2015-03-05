package com.stratio.connector.hdfs.scala.utils

import com.stratio.connector.hdfs.configuration.HDFSConstants
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.exceptions.ConnectionException
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import org.apache.hadoop.conf.Configuration

class HDFSClient (config: Configuration)  {

  private val logger = LoggerFactory.getLogger(getClass)

  def ifExists(source:Path): Boolean ={
    val hdfs:FileSystem = FileSystem.get(config)
    val ifExists = hdfs.exists(source)
    return ifExists
  }

  def copyFromLocal(source:String, dest:String): Unit ={
    val fileSystem:FileSystem = FileSystem.get(config)
    val srcPath: Path = new Path (source)
    val dstPath: Path = new Path(dest)

    //Check if the file already exists
    if (!(fileSystem.exists(dstPath)))
      logger.info("No such destination " + dstPath)

  }

}

object HDFSClient {

  def apply(host:String): HDFSClient = new HDFSClient({
    val config = new Configuration()
    config.set(PropName, HDFSUriScheme + "://" + host)
    config
  })

  def apply(clusterConfig: ConnectorClusterConfig): HDFSClient =
    new HDFSClient({
      val config = new Configuration()
      val clusterOptions = clusterConfig.getClusterOptions
      config.set(PropName, HDFSUriScheme + "://" + HostPort)
      if(ConfigDifferentPartitions.equals(Partitions))
        TableInDifferentPartitions = true
      else if (ConfigOnePartition.equals(Partitions))
        TableInDifferentPartitions = false
      else
        throw new ConnectionException(
          "The value " + clusterOptions.get(Partitions) +
          " stored in the configuration option " + Partitions + " is not valid.")
      if(clusterOptions.get(ConfigPartitionName) != null)
        PartitionName = clusterOptions.get(ConfigPartitionName)
      if(clusterOptions.get(ConfigExtensionName) != null)
        Extension = clusterOptions.get(ConfigExtensionName)
      if(clusterOptions.get(FileSeparator) != null)
        Separator = clusterOptions.get(FileSeparator)
      config
    })

  val MapSize: Int = 4 * 1024 // 4K - make this * 1024 to 4MB in a real system
  val Replication: Short = 1
  val PropName: String = "fs.default.name"
  val DefaultExtension: String = ".csv"

  var Separator: String = ","
  var TableInDifferentPartitions: Boolean = false
  var PartitionName: String = "part"
  var Extension: String = ".csv"

  val MethodNotSupported: String = "Not supported yet"
  val HostPort: String = "Host"
  val Partitions: String = "Partitions"
  val ConfigOnePartition: String = "OnePartition"
  val ConfigDifferentPartitions: String = "DifferentPartitions"
  val ConfigPartitionName: String = "PartitionName"
  val ConfigExtensionName: String = "Extension"
  val FileSeparator: String = "FileSeparator"
  val HDFSUriScheme: String = "com.stratio.connector.hdfs.scala"
}