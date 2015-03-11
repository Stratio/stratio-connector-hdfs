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

class HDFSClient (val hdfs: FileSystem,
  val compressionCodec: CompressionCodecName = CompressionCodecName.SNAPPY)
  extends HDFSConstants {

  private val logger = LoggerFactory.getLogger(getClass)

  def copyFromLocal(source: String, dest: String): Try[Unit] = {

    val srcPath: Path = new Path(source)
    val dstPath: Path = new Path(dest)

    //Get the filename from the file path
    val filename = source.substring(source.lastIndexOf('/') + 1, source.length)

    Try {
      //Check if the file already exists
      if (!(hdfs.exists(dstPath)))
        throw new XDExecutionException(s"The path $dstPath does not exist.")

      hdfs.copyFromLocalFile(srcPath, dstPath)
      hdfs.close()
    }.recover {
      case cause => throw new XDExecutionException(s"Unable to copy the file" +
        s" $srcPath to the destination path $dstPath.", cause)
    }

  }

  def addFileFromOrigin(source: String, dest: String): Unit = {

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
      if (hdfs.exists(path))
        logger.info("File " + dest + " already exists")

      // Create a new file and write data to it.
      val out: FSDataOutputStream = hdfs.create(path)

      val in: InputStream =
        new BufferedInputStream(new FileInputStream(new File(source)))

      IOUtils.copyBytes(in, out, hdfs.getConf)

      // Close all the file descripters
      in.close
      out.close
    }.recover {
      //case e:Exception => logger.error(s"Exception caught ! : ${e.getMessage}")
      case _ => throw new XDExecutionException(s"Unable to copy bytes " +
        s"from $source to $dest")
    }
  }

//  def addFile (dest:String): Unit = {
//    Try{
//
//      // Get the filename out of the file path.
//      val filename: String =
//        dest.substring(dest.lastIndexOf('/') + 1, dest.length)
//
//      //Create the destination path including the filename.
//      if(table)
//    }
//  }

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

      if (!hdfs.exists(path)) {
        throw new XDExecutionException("File " + dest + " does not exist")
      }

      val out: FSDataOutputStream = hdfs.append(path)
      val in: InputStream =
        new BufferedInputStream(new ByteArrayInputStream(source.getBytes))

      IOUtils.copyBytes(in, out, hdfs.getConf)

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
    if (!hdfs.exists(path)) {
      val message = s"File $filePath does not exist"
      logger.error(message)
      Failure(new XDExecutionException(message))
    }
    else {

      import com.stratio.connector.hdfs.scala.HDFSClient._
      val reader = new BufferedReader(
        new InputStreamReader(hdfs.open(path)))
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

  def createFolder (path: String): Unit ={
    val hdfsPath = new Path(path)
    if (!hdfs.exists(hdfsPath))
      hdfs.mkdirs(hdfsPath)
  }
}


object HDFSClient extends HDFSConstants{

  def apply(hostPort:String): HDFSClient = new HDFSClient({

    val config = new Configuration()

    config.set(PropName, HDFSUriScheme + "://" + hostPort)
    FileSystem.get(config)
  })

  def apply(clusterConfig: ConnectorClusterConfig): HDFSClient =

    new HDFSClient({

      val config = new Configuration()
      val clusterOptions = clusterConfig.getClusterOptions
      config.set(PropName, HDFSUriScheme + "://" + HostPort)
      FileSystem.get(config)
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
  //TODO THIS IS NOT A CONSTANT THIS HAS TO BE A CONNECTOR PROPERTY
  val HostPort: String = "conectores3:9000"
  val HDFSUriScheme: String = "hdfs"

}