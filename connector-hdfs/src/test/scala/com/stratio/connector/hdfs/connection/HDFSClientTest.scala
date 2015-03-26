package com.stratio.connector.hdfs.connection

import java.util

import com.stratio.connector.hdfs.engine.FSConstants
import com.stratio.crossdata.common.connector.ConnectorClusterConfig
import com.stratio.crossdata.common.data.ClusterName
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, FlatSpec}
import org.scalamock.scalatest.MockFactory


class HDFSClientTest extends FlatSpec with Matchers with FSConstants with MockFactory{

  behavior of "HDFS Client"

  it should "Create a new folder if it doesn't exist on the specific path" in {

    import scala.collection.JavaConversions._

    val clusterName = new ClusterName("ClusterName")

    val connectorOptions = new util.HashMap[String, String]()

    val clusterOptions: Map[String,String] = Map("hosts" -> "10.200.0.60:9000")

    val connectorClusterConfig = new ConnectorClusterConfig(clusterName, connectorOptions, clusterOptions)

    val path = "/home/hrodriguez/createFolder"

    val hdfsPath = new Path (path)

    val hdfsClient = HDFSClient(connectorClusterConfig)

    hdfsClient.createFolder(path)

    fakeFileSystem.exists(hdfsPath) should equal (true)
  }
}
