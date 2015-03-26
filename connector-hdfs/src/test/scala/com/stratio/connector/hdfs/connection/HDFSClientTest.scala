package com.stratio.connector.hdfs.connection


import com.stratio.connector.hdfs.engine.FSConstants
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, FlatSpec}
import org.scalamock.scalatest.MockFactory


class HDFSClientTest extends FlatSpec with Matchers with FSConstants with MockFactory{

  behavior of "HDFS Client"

  it should "Create a new folder if it doesn't exist on the specific path" in {

    val path = "folderTest"

    val hdfsPath = new Path (path)

    hDFSClient.createFolder(path)

    fakeFileSystem.exists(hdfsPath) should equal (true)

    fakeFileSystem.delete(hdfsPath, true)
  }
}
