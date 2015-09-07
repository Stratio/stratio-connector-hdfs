First Steps
***********

Table of Contents
=================

-  `Before you start <#before-you-start>`__

   -  `Prerequisites <#prerequisites>`__
   -  `Configuration <#configuration>`__

-  `Creating the catalog and the
   collection <#creating-the-catalog-and-the-collection>`__

   -  `Step 1: Creating the catalog <#step-1-creating-the-catalog>`__
   -  `Step 2: Creating the collection <#step-2-creating-the-collection>`__

-  `Inserting Data <#inserting-data>`__

   -  `Step 3: Insert into collection
      students <#step-3-insert-into-collection-students>`__

-  `Where to go from here <#where-to-go-from-here>`__

Before you start
================

Prerequisites
-------------

-  First of all `Stratio Crossdata <http://docs.stratio.com/modules/crossdata/0.4/index.html>`__ is needed and must be
   installed. The server and the shell must be running.
-  Installation of
   `HDFS 2.6 <http://hadoop.apache.org/docs/r2.6.0>`__.
-  Build an HDFSConnector executable and run it following this
   `guide <http://docs.stratio.com/modules/stratio-connector-hdfs/0.5/about.html>`__.

Configuration
-------------

In the Crossdata Shell, if high availability configuration is required:

::

    >  ATTACH CLUSTER hdfsCluster ON DATASTORE hdfs WITH OPTIONS {'highavailability': 'true', 'path':'Base_path_to_HDFS_root_directory'};

The output must be:

::

      Result: QID: 82926b1e-2f72-463f-8164-98969c352d40
      Cluster attached successfully

If high availability is not required:

::

    >  ATTACH CLUSTER hdfsCluster ON DATASTORE hdfs WITH OPTIONS {'highavailability': 'false', 'path':'Base_path_to_HDFS_root_directory', 'hosts': 'Host_and_port_of_the_namenode'};

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

Creating the catalog and the collection
====================================

Step 1: Creating the catalog
----------------------------

Now we will create the catalog and the table which we will use later in
the next steps.

To create the catalog we must execute.

::

        > CREATE CATALOG highschool;

The output must be:

::

    CATALOG created successfully;

Step 2: Creating the collection
-------------------------------

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

To learn more about Stratio Crossdata, we recommend you to visit the
`Stratio Crossdata Reference <http://docs.stratio.com/modules/crossdata/0.4/index.html>`__.
