About
=====

The Stratio Connector-HDFS allows `Stratio Crossdata <http://docs.stratio.com/modules/crossdata/0.4/index.html>`__ to interact with HDFS.

Requirements
------------

`Crossdata <http://docs.stratio.com/modules/crossdata/0.4/index.html>`__ is needed to interact with this connector.

Compiling Stratio Connector-HDFS
--------------------------------

To automatically build execute the following commands:

::

       > mvn clean compile install


Running the Stratio Connector-HDFS
----------------------------------

To run Stratio Connector-HDFS execute in the parent directory:

::

       > ./connector-hdfs/target/stratio-connector-hdfs/bin/stratio-connector-hdfs


Build a redistributable package
-------------------------------

It is possible too, to create a RPM or DEB package, as :

::

    > mvn package -Ppackage

Once the package it’s created, execute this commands to install:

RPM Package:

::

    > rpm -i target/stratio-connector-hdfs-[version].rpm

DEB Package:

::

    > dpkg -i target/stratio-connector-hdfs-[version].deb

Now to start/stop the connector:

::

    > service stratio-connector-hdfs start

    > service stratio-connector-hdfs stop


How to use Stratio Connector-HDFS
---------------------------------

A complete tutorial is available `here <First_Steps.html>`__. The
basic commands are described below.

1. Start `Stratio Crossdata Server and then Stratio Crossdata Shell <http://docs.stratio.com/modules/crossdata/0.4/index.html>`__.

2. Start Stratio Connector-HDFS as it is explained before.

3. In the Stratio Crossdata Shell:

   Attach a cluster on that datastore:

   ::

   xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'highavailability': 'true', 'path':'Base_path_to_HDFS_root_directory'};``

   Attach the connector to the previously defined cluster. The connector
   name must match the one defined in the Connector Manifest.

   ::

   xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {};


At this point, we can start to send queries in the Stratio Crossdata Shell.

License
=======

Stratio Connector-HDFS is licensed as
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
