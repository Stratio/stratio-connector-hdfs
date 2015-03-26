First Steps
***********

HDFS Crossdata connector allows the integration between Crossdata and
HDFS for writing with Parquet format. Crossdata provides an easy and common language as well as the
integration with several other databases. More information about
Crossdata can be found at
`Crossdata. <https://github.com/Stratio/crossdata>`__

Table of Contents
=================

-  `Before you start <#before-you-start>`__

   -  `Prerequisites <#prerequisites>`__
   -  `Configuration <#configuration>`__

-  `Creating the database and
   collection <#creating-the-database-and-collection>`__

   -  `Step 1: Create the database <#step-1-create-the-database>`__
   -  `Step 2: Create the collection <#step-2-create-the-collection>`__

-  `Inserting Data <#inserting-data>`__

   -  `Step 3: Insert into collection
      students <#step-3-insert-into-collection-students>`__

-  `Where to go from here <#where-to-go-from-here>`__

Before you start
================

Prerequisites
-------------

-  Basic knowledge of SQL like language.
-  First of all `Stratio Crossdata
   0.2.0-001 <https://github.com/Stratio/crossdata/tree/0.2.0-001>`__ is needed and must be
   installed. The server and the shell must be running.
-  An installation of
   `HDFS 2.6 <http://hadoop.apache.org/docs/r2.6.0>`__.
-  Build an HDFSConnector executable and run it following this
   `guide <http://localhost:63342/stratio-connector-hdfs-parent/stratio-connector-hdfs-doc/target/site/html/about.html>`__.

Configuration
-------------

In the Crossdata Shell we need to add the Datastore Manifest.

::

       > ADD DATASTORE "<path_to_manifest_folder>/HDFSDataStore.xml";

Now we need to add the ConnectorManifest.

::

       > ADD CONNECTOR "<path_to_manifest_folder>/HDFSConnector.xml";

At this point we have reported to Crossdata the connector options and
operations. Now we configure the datastore cluster.

If high availability is required:

::

    >  ATTACH CLUSTER hdfsCluster ON DATASTORE hdfs WITH OPTIONS {'highavailability': 'true', 'user': 'The_user_name', 'path':'Base_path_to_HDFS_root_directory'};

The output must be:

::

      Result: QID: 82926b1e-2f72-463f-8164-98969c352d40
      Cluster attached successfully

If high availability is not required:

::

    >  ATTACH CLUSTER hdfsCluster ON DATASTORE hdfs WITH OPTIONS {'highavailability': 'false', 'user': 'The_user_name', 'path':'Base_path_to_HDFS_root_directory', 'hosts': 'Host_and_port_for_the_namenode'};

.. warning::

You must add core-site.xml and hdfs-site.xml into the config folder (src/main/config) if you require high availability. If these folders are added in the config folder, the connector will take this configuration by default.

Now we run the connector.

The last step is to attach the connector to the cluster created before.

::

      >  ATTACH CONNECTOR hdfsconnector TO hdfsCluster  WITH OPTIONS {};

The output must be:

::

    CONNECTOR attached successfully

To ensure that the connector is online we can execute the Crossdata
Shell command:

::

      > describe connectors;

And the output must show a message similar to:

::

    Connector: connector.hdfsconnector  ONLINE  []  [datastore.hdfs]    akka.tcp://CrossdataServerCluster@127.0.0.1:46646/user/ConnectorActor/

Creating the database and collection
====================================

Step 1: Create the database
---------------------------

Now we will create the catalog and the table which we will use later in
the next steps.

To create the catalog we must execute.

::

        > CREATE CATALOG highschool;

The output must be:

::

    CATALOG created successfully;

Step 2: Create the collection
-----------------------------

To create the table we must execute the next command.

::

      > CREATE TABLE highschool.students ON CLUSTER hdfsCluster (id int PRIMARY KEY, name text, age int, enrolled boolean);

And the output must show:

::

    TABLE created successfully

Inserting Data
==============

Step 3: Insert into collection students
---------------------------------------

At first we must insert some rows in the table created before.

::

      >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (1, 'Jhon', 16, true);
      >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (2, 'Eva', 20, true);
      >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (3, 'Lucie', 18, true);
      >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (4, 'Cole', 16, true);
      >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (5, 'Finn', 17, false);
      >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (6, 'Violet', 21, false);
      >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (7, 'Beatrice', 18, true);
      >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (8, 'Henry', 16, false);
      

For each row the output must be:

::

    STORED successfully

Where to go from here
=====================

To learn more about Stratio Crossdata, we recommend to visit the
`Crossdata
Reference <http://docs.stratio.com/crossdata>`__.

