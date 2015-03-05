package com.stratio.connector.hdfs.scala.connection

import com.stratio.connector.commons.connection.{Connection, ConnectionHandler}
import com.stratio.crossdata.common.connector.{ConnectorClusterConfig, IConfiguration}
import com.stratio.crossdata.common.security.ICredentials

class HDFSConnectionHandler(iConfiguration: IConfiguration)
  extends ConnectionHandler(iConfiguration){
  override def createNativeConnection(
    credentials: ICredentials,
    config: ConnectorClusterConfig): Connection[_] =
    new HDFSConnection(credentials,config)
}
