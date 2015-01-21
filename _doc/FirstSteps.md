# First Steps #

HDFS Crossdata connector allows the integration between Crossdata and HDFS. Crossdata provides an easy and common language as well as the integration with several other databases. More information about Crossdata can be found at [Crossdata](https://github.com/Stratio/crossdata)

Table of Contents
=================

-   [Before you start](#before-you-start)
    -   [Prerequisites](#prerequisites)
    -   [Configuration](#configuration)
-   [Creating the database and collection](#creating-the-database-and-collection)
    -   [Step 1: Create the database](#step-1-create-the-database)
    -   [Step 2: Create the collection](#step-2-create-the-collection)
-   [Inserting Data](#inserting-data)
    -   [Step 3: Insert into collection students](#step-3-insert-into-collection-students)
    	-   [Insert if not exists](#insert-if-not-exists)
-   [Delete Data and Remove schemas](#delete-data-and-remove-schemas)
	-	[Step 4: Truncate Collection](#step-4-truncate-collection)
    -   [Step 5: Drop Collection](#step-5-drop-collection)
    -   [Step 6: Drop Database](#step-6-drop-database)
-   [Where to go from here](#where-to-go-from-here)

Before you start
================

Prerequisites
-------------

-   Basic knowledge of SQL like language.
-   First of all [Stratio Crossdata 0.2.0](https://github.com/Stratio/crossdata) is needed and must be installed. The server and the shell must be running.
-   An installation of [HDFS](http://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html#HDFS_Users_Guide). 
-   Build a HDFSConnector executable and run it following this [guide](https://github.com/Stratio/stratio-connector-hdfs#build-an-executable-connector-HDFS). 

Configuration
-------------


In the Crossdata Shell we need to add the Datastore Manifest.

```
   > add datastore "<path_to_manifest_folder>/HDFSDataStore.xml";
```

The output must be:

```
   [INFO|Shell] CrossdataManifest added 
	DATASTORE
	Name: hdfs
	Version: 2.4.1
	Required properties: 
	Property: 
		PropertyName: Hosts
		Description: The list of hosts ips (csv). Example: host1,host2,host3
	Property: 
		PropertyName: Port
		Description: The list of ports (csv).
	Property: 
		PropertyName: Partitions
		Description: Structure of the HDFS 
	Property: 
		PropertyName: PartitionName
		Description: Name of the File for the different partitions of the table.
	Property: 
		PropertyName: Extension
		Description: Extension of the file in HDFS.
	Property: 
		PropertyName: FileSeparator
		Description: Character to split the File lines


```

Now we need to add the ConnectorManifest.

```
   > add connector "<path_to_manifest_folder>/HDFSConnector.xml";
```
The output must be:


```
   [INFO|Shell] CrossdataManifest added 
    CONNECTOR
    ConnectorName: hdfsconnector
   DataStores: 
	DataStoreName: hdfs
    Version: 0.2.0
    Supported operations:
							.
							.
							.

```
At this point we have reported to Crossdata the connector options and operations. Now we configure the datastore cluster.

```
>  ATTACH CLUSTER hdfsCluster ON DATASTORE hdfs WITH OPTIONS {'Hosts': '[Ip1, Ip2,..,Ipn]', 
'Port': '[Port1,Port2,...,Portn]'};
```

The output must be similar to:
```
  Result: QID: 82926b1e-2f72-463f-8164-98969c352d40
  Cluster attached successfully
```

Now we run the connector.

The last step is to attach the connector to the cluster created before.

```
  >  ATTACH CONNECTOR hdfsconnector TO hdfsCluster  WITH OPTIONS {};
```

The output must be:
```
CONNECTOR attached successfully
```

To ensure that the connector is online we can execute the Crossdata Shell command:

```
  > describe connectors;
```

And the output must show a message similar to:

```
Connector: connector.hdfsconnector	ONLINE	[]	[datastore.hdfs]	akka.tcp://CrossdataServerCluster@127.0.0.1:46646/user/ConnectorActor/
```


Creating the database and collection
===============================

Step 1: Create the database
---------------------------

Now we will create the catalog and the table which we will use later in the next steps.

To create the catalog we must execute.

```
    > CREATE CATALOG highschool;
```
The output must be:

```
CATALOG created successfully;
```

Step 2: Create the collection
--------------------------------

We switch to the database we have just created.

```
  > USE highschool;
```

To create the table we must execute the next command.

```
  > CREATE TABLE students ON CLUSTER hdfsCluster (id int PRIMARY KEY, name text, age int, enrolled boolean);
```

And the output must show:

```
TABLE created successfully
```


Inserting Data
==============

Step 3: Insert into collection students
-------------------------------

At first we must insert some rows in the table created before.
```
  >  INSERT INTO students(id, name,age,enrolled) VALUES (1, 'Jhon', 16, true);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (2, 'Eva', 20, true);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (3, 'Lucie', 18, true);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (4, 'Cole', 16, true);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (5, 'Finn', 17, false);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (6, 'Violet', 21, false);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (7, 'Beatrice', 18, true);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (8, 'Henry', 16, false);
  
```

For each row the output must be:

```
STORED successfully
```


Delete Data and Remove Schemas
==============================
Step 4: Truncate Collection
-------------------

To truncate the table we must execute:
```
	>  TRUNCATE students;
  	STORED successfully
```


Step 5: Drop Collection
-------------------

To drop the table we must execute:
```
  >  DROP TABLE students;
  TABLE dropped successfully

```

Step 6: Drop database
----------------------

```
  >  DROP CATALOG highschool;
  CATALOG dropped successfully

```

Where to go from here
=====================

To learn more about Stratio Crossdata, we recommend to visit the [Crossdata Reference](https://github.com/Stratio/crossdata/tree/master/_doc/meta-reference.md "Meta Reference").

