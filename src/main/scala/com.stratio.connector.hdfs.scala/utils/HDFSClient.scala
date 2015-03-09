package com.stratio.connector.hdfs.scala.utils

import java.io._
import java.util.concurrent.ExecutionException

import com.stratio.connector.hdfs.configuration.HDFSConstants
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.exceptions.{
InitializationException,
ConnectionException,
ExecutionException => XDExecutionException}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.slf4j.LoggerFactory
import org.apache.hadoop.conf.Configuration

import scala.util.Try

class HDFSClient (config: Configuration) extends HDFSConstants {

  private val logger = LoggerFactory.getLogger(getClass)

  def ifExists(source: Path): Boolean = {
    val hdfs: FileSystem = FileSystem.get(config)
    val ifExists = hdfs.exists(source)
    return ifExists
  }

  def copyFromLocal(source: String, dest: String): Try[Unit] = {

    val srcPath: Path = new Path(source)
    val dstPath: Path = new Path(dest)

    //Get the filename from the file path
    val filename = source.substring(source.lastIndexOf('/') + 1, source.length)

    Try {
      val fileSystem: FileSystem = FileSystem.get(config)

      //Check if the file already exists
      if (!(fileSystem.exists(dstPath)))
        throw new XDExecutionException(s"The path $dstPath does not exist.")

      fileSystem.copyFromLocalFile(srcPath, dstPath)
      fileSystem.close()
    }.recover {
      //case e:Exception => logger.error(s"Exception caught ! : ${e.getMessage}")
      //???HACER SUCESS AND FAILURE???
      case cause => throw new XDExecutionException(s"Unable to copy the file" +
        s" $srcPath to the destination path $dstPath.", cause)
    }

  }

  def addFile(source: String, dest: String): Unit = {

    Try {
      val fileSystem: FileSystem = FileSystem.get(config)

      // Get the filename out of the file path
      val filename: String = source.substring(source.lastIndexOf('/') + 1, source.length)

      // Create the destination path including the filename
      if (dest.charAt(dest.length - 1) != '/') {
        val completeDest = dest + "/" + filename
      } else {
        val completeDest = dest + filename
      }

      // Check if the file already exists
      val path: Path = new Path(dest)
      if (fileSystem.exists(path))
        logger.info("File " + dest + " already exists")

      // Create a new file and write data to it.
      val out: FSDataOutputStream = fileSystem.create(path)

      val in: InputStream = new BufferedInputStream(new FileInputStream(new File(source)))

      IOUtils.copyBytes(in, out, config)

      // Close all the file descripters
      in.close
      out.close
      fileSystem.close

    }.recover {
      //case e:Exception => logger.error(s"Exception caught ! : ${e.getMessage}")
      //???
      case _ => throw new ExecutionException(s"Unable to copy bytes " +
        s"from $source to $dest") //???
    }
  }

  def addRowToFile(source: String, dest: String): Unit = {

    Try {

      val fileSystem: FileSystem = FileSystem.get(config)
      import HDFSClient._
      if (TableInDifferentPartitions) {
        val filename: String = PartitionName + Extension
        if (dest.charAt(dest.length - 1) != '/') {
          val completeDest = dest + "/" + filename
        } else {
          val completeDest = dest + filename
        }
      } else {
        val completeDest = dest + Extension
      }
      val path: Path = new Path(dest)
      if (!fileSystem.exists(path)) {
        throw new ExecutionException("File " + dest + " does not exist")
      }
      val out: FSDataOutputStream = fileSystem.append(path)
      val in: InputStream = new BufferedInputStream(new ByteArrayInputStream(source.getBytes))
      IOUtils.copyBytes(in, out, config)
      in.close
      out.close
      fileSystem.close
    }.recover {
      //case e:Exception => logger.error(s"Exception caught ! : ${e.getMessage}")
      //???
      case _ => throw new ExecutionException(s"Unable to copy bytes " +
        s"from $source to $dest") //???
    }
  }
}




object HDFSClient extends HDFSConstants{ //???

  def apply(hostPort:String): HDFSClient = new HDFSClient({
    val config = new Configuration()
    config.set(PropName, HDFSUriScheme + "://" + hostPort)
    config
  })

  def apply(clusterConfig: ConnectorClusterConfig): HDFSClient =
    new HDFSClient({

      val config = new Configuration()
      val clusterOptions = clusterConfig.getClusterOptions
      if (clusterOptions.get(HostPort).split(":").size != 2){
        throw new InitializationException(
          s"The value ${clusterOptions.get(HostPort)} stored " +
            s"in the configuration option $HostPort is not valid.")
      }

      config.set(PropName, HDFSUriScheme + "://" + HostPort)

      if(ConfigDifferentPartitions.equals(Partitions))
        TableInDifferentPartitions = true
      else if (ConfigOnePartition.equals(Partitions))
        TableInDifferentPartitions = false
      else {
        throw new ConnectionException(
          s"The value  ${clusterOptions.get(Partitions)} stored " +
            s"in the configuration option $Partitions is not valid.")
      }
      if(clusterOptions.get(ConfigPartitionName) != null)
        PartitionName = clusterOptions.get(ConfigPartitionName)
      if(clusterOptions.get(ConfigExtensionName) != null)
        Extension = clusterOptions.get(ConfigExtensionName)
      if(clusterOptions.get(FileSeparator) != null)
        Separator = clusterOptions.get(FileSeparator)
      config
    })


}

private[utils] trait  HDFSConstants {

  var Separator: String = ","
  var TableInDifferentPartitions: Boolean = false
  var PartitionName: String = "part"
  var Extension: String = ".csv"//???


  //Configuration constants
  val MapSize: Int = 4 * 1024 //4K - make this * 1024 to 4MB in a real system
  val Replication: Short = 1
  val PropName: String = "fs.default.name"
  val DefaultExtension: String = ".csv"

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