# About #

Native connector for HDFS using Crossdata.

## Requirements ##

[Stratio HDFS](https://github.com/Stratio/stratio-connector-hdfs) must be installed and started.
[Crossdata] (https://github.com/Stratio/crossdata) is needed to interact with this connector.

## Compiling Stratio Connector HDFS ##

To automatically build execute the following command:

```
   > mvn clean compile install
```

## Build an executable Connector HDFS ##

To generate the executable, execute the following command:

```
   > mvn crossdata-connector:install
```   
It is possible to add new special HDFS configuration:

The file called HDFSDataStore.xml contains some properties. The "Partitions" variable could be set to two different configurations: 

Set this variable to "DiferentPartitions" in the ConnectorClusterConfig options to create a HDFS structure with different file
partitions of the data table:

    catalog/table/partition1.xxx
                 /partition2.xxx
                 /partition3.xxx
                 
Set this variable to "OnePartition" in the ConnectorClusterConfig options to create a HDFS structure with one file partition of the data table;

    catalog/table1.xxx
           /table2.xxx
           /table3.xxx
                 

In order to specify the partition sequence use the "PartitionName" variable in the configuration options.
In order to specify the extension of the files use the "Extension" variable in the configuration options.

   
## Running the Stratio Connector HDFS ##

To run HDFS Connector execute:

```
   > mvn exec:java -Dexec.mainClass="com.stratio.connector.hdfs.connection.HDFSConnector"
```

## How to use HDFS Connector ##

A complete tutorial is available [here](_doc/FirstSteps.md). The basic commands are described below.

 1. Start [crossdata-server and then crossdata-shell](https://github.com/Stratio/crossdata).
 2. https://github.com/Stratio/crossdata
 3. Start HDFS Connector as it is explained before
 4. In crossdata-shell:

    Add a datastore with this command.  We need to specified the XML manifest that defines the data store. The XML manifest can be found in the path of the HDFS Connector in target/stratio-connector-hdfs-0.2.0/conf/HDFSDataStore.xml

      ```
         xdsh:user>  ADD DATASTORE <Absolute path to HDFS Datastore manifest>;
      ```

    Attach cluster on that datastore. The datastore name must be the same as the defined in the Datastore manifest.

      ```
        xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'Hosts': '[<IPHost_1,IPHost_2,...,IPHost_n>]', 'Port': '[<PortHost_1,PortHost_2,...,PortHost_n>]'};
      ```

    Add the connector manifest. The XML with the manifest can be found in the path of the HDFS Connector in target/stratio-connector-hdfs-0.2.0/conf/HDFSConnector.xml

       ```
         xdsh:user>  ADD CONNECTOR <Path to HDFS Connector Manifest>
       ```

    Attach the connector to the previously defined cluster. The connector name must match the one defined in the
    Connector Manifest.

        ```
            xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {};
        ```

    At this point, we can start to send queries.

        ...
            xdsh:user> CREATE CATALOG catalogTest;

            xdsh:user> USE catalogTest;

            xdsh:user> CREATE TABLE tableTest ON CLUSTER hdfs_prod (id int PRIMARY KEY, name text);

        ...


# License #

Stratio Crossdata is licensed as [Apache2](http://www.apache.org/licenses/LICENSE-2.0.txt)

Licensed to STRATIO (C) under one or more contributor license agreements.
See the NOTICE file distributed with this work for additional information
regarding copyright ownership.  The STRATIO (C) licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
