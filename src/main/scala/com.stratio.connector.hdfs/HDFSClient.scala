package com.stratio.connector.hdfs.scala

import java.io._

import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.data.ColumnName
import com.stratio.crossdata.common.exceptions.{ConnectionException, ExecutionException => XDExecutionException, InitializationException}
import com.stratio.crossdata.common.metadata.{ColumnMetadata, ColumnType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.slf4j.LoggerFactory
import parquet.hadoop.metadata.CompressionCodecName

import scala.util.{Failure, Success, Try}

class HDFSClient (val hdfs: Option[FileSystem],
  val compressionCodec: CompressionCodecName = CompressionCodecName.SNAPPY)
  extends HDFSConstants {

  private val logger = LoggerFactory.getLogger(getClass)

  def ifExists(source: Path): Boolean = {
    val ifExists = hdfs.get.exists(source)
    return ifExists
  }

  def copyFromLocal(source: String, dest: String): Try[Unit] = {

    val srcPath: Path = new Path(source)
    val dstPath: Path = new Path(dest)

    //Get the filename from the file path
    val filename = source.substring(source.lastIndexOf('/') + 1, source.length)

    Try {
      //Check if the file already exists
      if (!(hdfs.get.exists(dstPath)))
        throw new XDExecutionException(s"The path $dstPath does not exist.")

      hdfs.get.copyFromLocalFile(srcPath, dstPath)
      hdfs.get.close()
    }.recover {
      case cause => throw new XDExecutionException(s"Unable to copy the file" +
        s" $srcPath to the destination path $dstPath.", cause)
    }

  }

  def addFile(source: String, dest: String): Unit = {

    Try {
      // Get the filename out of the file path
      val filename: String =
        source.substring(source.lastIndexOf('/') + 1, source.length)

      // Create the destination path including the filename
      if (dest.charAt(dest.length - 1) != '/') {
        val completeDest = dest + "/" + filename
      } else {
        val completeDest = dest + filename
      }

      // Check if the file already exists
      val path: Path = new Path(dest)
      if (hdfs.get.exists(path))
        logger.info("File " + dest + " already exists")

      // Create a new file and write data to it.
      val out: FSDataOutputStream = hdfs.get.create(path)

      val in: InputStream =
        new BufferedInputStream(new FileInputStream(new File(source)))

      IOUtils.copyBytes(in, out, hdfs.get.getConf)

      // Close all the file descripters
      in.close
      out.close
    }.recover {
      //case e:Exception => logger.error(s"Exception caught ! : ${e.getMessage}")
      case _ => throw new XDExecutionException(s"Unable to copy bytes " +
        s"from $source to $dest")
    }
  }

  def addRowToFile(source: String, dest: String): Unit = {

    Try {
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

      if (!hdfs.get.exists(path)) {
        throw new XDExecutionException("File " + dest + " does not exist")
      }

      val out: FSDataOutputStream = hdfs.get.append(path)
      val in: InputStream =
        new BufferedInputStream(new ByteArrayInputStream(source.getBytes))

      IOUtils.copyBytes(in, out, hdfs.get.getConf)

      in.close

      out.close
    }.recover {
      case _ => throw new XDExecutionException(s"Unable to copy bytes " +
        s"from $source to $dest")
    }
  }


  def getMetaDataInfoFromFile (filePath: String):
  Try[Map[ColumnName, ColumnMetadata]] = {

    val metaDataFile: StringBuilder = new StringBuilder

    val filePathBeginning = filePath.substring(0, filePath.lastIndexOf('/'))

    if (TableInDifferentPartitions) {
      val filePathComplete =
        filePathBeginning + "/metaFile/" + PartitionName + ".csv"
    }
    else {
      val filePathComplete = filePathBeginning + "/metaFile.csv"
    }

    val path = new Path(filePath)
    if (!hdfs.get.exists(path)) {
      val message = s"File $filePath does not exist"
      logger.error(message)
      Failure(new XDExecutionException(message))
    }
    else {

      import com.stratio.connector.hdfs.scala.HDFSClient._
      val reader = new BufferedReader(
        new InputStreamReader(hdfs.get.open(path)))
      val file = toStream(reader)

      type CatalogName = String
      type TableName = String
      type Fields = Map[ColumnName, ColumnMetadata]

      val zero: (CatalogName, TableName, Fields) = ("", "", Map())

      val (_, _, columns) = (zero /: file) {

        case ((catalogName, tableName, fields), line)
          if line.startsWith("CatalogName=") =>
          (line.split("=").tail.head, tableName, fields)

        case ((catalogName, tableName, fields), line)
          if line.startsWith("TableName=") =>
          (catalogName, line.split("=").tail.head, fields)

        case ((catalogName, tableName, fields), line)
          if line.startsWith("Field") =>

          val (_ :: columnName :: columnType :: _) = line.split(":").toList

          val colName = new ColumnName(catalogName, tableName, columnName)
          (catalogName, tableName, fields + (colName ->
            new ColumnMetadata(
              colName,
              Array(),
              ColumnType.valueOf(columnType))))

        case (previous, _) => previous
      }

      Success(columns)

    }
  }
}


object HDFSClient extends HDFSConstants{

  def apply(hostPort:String): HDFSClient = new HDFSClient({

    val config = new Configuration()

    config.set(PropName, HDFSUriScheme + "://" + hostPort)
    Some(FileSystem.get(config))
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

      Some(FileSystem.get(config))
    })

  //  Helpers

  def toStream(reader: BufferedReader): Stream[String] = {

    lazy val fileStream: Stream[Try[String]] =
      Try(reader.readLine()) #:: fileStream.map(_ => Try(reader.readLine()))

    fileStream.takeWhile(_.isSuccess).map(_.get)
  }

}

private[hdfs] trait  HDFSConstants {

  var Separator: String = ","
  var TableInDifferentPartitions: Boolean = false
  var PartitionName: String = "part"
  var Extension: String = ".csv"//???


  //Configuration constants
  val MapSize: Int = 4 * 1024 //4K - make this * 1024 to 4MB in a real system
  val Replication: Short = 1
  val PropName: String = "fs.default.name"
  val DefaultExtension: String = ".csv"

  val HostPort: String = "Host"
  val Partitions: String = "Partitions"
  val ConfigOnePartition: String = "OnePartition"
  val ConfigDifferentPartitions: String = "DifferentPartitions"
  val ConfigPartitionName: String = "PartitionName"
  val ConfigExtensionName: String = "Extension"
  val FileSeparator: String = "FileSeparator"
  val HDFSUriScheme: String = "com.stratio.connector.hdfs"

}