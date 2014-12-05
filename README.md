stratio-connector-hdfs
======================


Add new special HDFS configuration :

Add "DiferentPartitions" to true in the ConnectorClusterConfig options to create hdfs structure with diferents file
partitions of the data table;

    catalog/table/partition1.xxx
                 /partition2.xxx
                 /partition3.xxx

to specify the partition sequence use the "PartitionName" in the the ConnectorClusterConfig options.
to specify the extension of the files use the "extension" in the the ConnectorClusterConfig options.

Add "OnePartition" to true in the ConnectorClusterConfig options to create hdfs structure with one file
partition of the data table;

    catalog/table1.xxx
           /table2.xxx
           /table3.xxx

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

## Running the Stratio Connector HDFS ##

```
   > mvn exec:java -Dexec.mainClass="com.stratio.connector.hdfs.HDFSConnector"
```


## How to use HDFS Connector ##

 1. Start [crossdata-server and then crossdata-shell](https://github.com/Stratio/crossdata).
 2. https://github.com/Stratio/crossdata
 3. Start HDFS Connector as it is explained before
 4. In crossdata-shell:

    Add a datastore with this command:

      ```
         xdsh:user>  ADD DATASTORE <Absolute path to HDFS Datastore manifest>;
      ```

    Attach cluster on that datastore. The datastore name must be the same as the defined in the Datastore manifest.

      ```
         xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'Hosts': '[<ipHost_1,
         ipHost_2,...ipHost_n>]', 'Port': <hdfs_port> };
      ```

    Add the connector manifest.

       ```
         xdsh:user>  ADD CONNECTOR <Path to HDFS Connector Manifest>
       ```

    Attach the connector to the previously defined cluster. The connector name must match the one defined in the
    Connector Manifest.

        ```
            xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {'DefaultLimit': '1000'};
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
