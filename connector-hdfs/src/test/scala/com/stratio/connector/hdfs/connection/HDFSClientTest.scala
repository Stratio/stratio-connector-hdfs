package com.stratio.connector.hdfs.connection


import com.stratio.connector.hdfs.UnitSpec
import org.apache.hadoop.fs.Path
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class HDFSClientTest extends UnitSpec{

  behavior of "HDFS Client"

  it should "Create a new folder if it doesn't exist on the specific path" in {

    val path = "folderTest"

    val hdfsPath = new Path (path)

    hDFSClient.createFolder(path)

    fakeFileSystem.exists(hdfsPath) should equal (true)

    fakeFileSystem.delete(hdfsPath, true)
  }
}
