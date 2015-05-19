           About
=====

Connector for writing in HDFS with Parquet format using Crossdata.

Requirements
------------

`Stratio HDFS <https://github.com/Stratio/stratio-connector-hdfs/tree/master>`__
must be installed and started. `Crossdata <https://github.com/Stratio/crossdata>`__ is needed to interact with this
connector.

Compiling Stratio Connector HDFS
--------------------------------

To automatically build execute the following commands:

::

       > mvn clean install

Build an executable Connector HDFS
----------------------------------

To generate the executable, execute the following command:

::

       > cd connector-hdfs/

::

       > mvn crossdata-connector:install

Running the Stratio Connector HDFS
----------------------------------

To run HDFS Connector execute:

::

       > target/stratio-connector-hdfs-[version]/bin/stratio-connector-hdfs-[version] start

Build a redistributable package
-------------------------------
It is possible too, to create a RPM or DEB redistributable package.

RPM Package:

::

       > mvn unix:package-rpm -N

DEB Package:

::
   
       > mvn unix:package-deb -N

Once the package is created, execute this commands to install:

RPM Package:

::   
    
       > rpm -i target/stratio-connector-hdfs-[version].rpm

DEB Package:

::   
    
       > dpkg -i target/stratio-connector-hdfs-[version].deb

Now, to start/stop the connector:

::   
    
       > service stratio-connector-hdfs start
       > service stratio-connector-hdfs stop


How to use HDFS Connector
-------------------------

A complete tutorial is available `here <http://localhost:63342/stratio-connector-hdfs-parent/stratio-connector-hdfs-doc/target/site/html/First_Steps.html>`__. The
basic commands are described below.

1. Start `crossdata-server and then
   crossdata-shell <http://docs.stratio.com/crossdata>`__.
2. Start HDFS Connector as it is explained before.
3. In crossdata-shell:

   Add a datastore. We need to specified the XML
   manifest that defines the data store. The XML manifest can be found
   in the path of the HDFS Connector in
   target/stratio-connector-hdfs-0.3.0/conf/HDFSDataStore.xml

   ``xdsh:user>  ADD DATASTORE <Absolute path to HDFS Datastore manifest>;``

   Attach a cluster on that datastore. The datastore name must be the same
   as the defined in the Datastore manifest.

   ``xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'highavailability': 'true', 'user': 'The_user_name', 'path':'Base_path_to_HDFS_root_directory'};``

   Add the connector manifest. The XML with the manifest can be found in
   the path of the HDFS Connector in
   target/stratio-connector-hdfs-0.3.0/conf/HDFSConnector.xml

   ``xdsh:user>  ADD CONNECTOR <Path to HDFS Connector Manifest>``

   Attach the connector to the previously defined cluster. The connector
   name must match the one defined in the Connector Manifest.

   ::

       ```
           xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {};
       ```

   At this point, we can start to send queries.

   ::

       ...
           xdsh:user> CREATE CATALOG catalogTest;

           xdsh:user> CREATE TABLE catalogTest.tableTest ON CLUSTER hdfs_prod (id int PRIMARY KEY, name text);

       ...

License
=======

Stratio Crossdata is licensed as
`Apache2 <http://www.apache.org/licenses/LICENSE-2.0.txt>`__

Licensed to STRATIO (C) under one or more contributor license
agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The STRATIO (C)
licenses this file to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

