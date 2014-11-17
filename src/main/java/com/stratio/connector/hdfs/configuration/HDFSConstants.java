package com.stratio.connector.hdfs.configuration;

import org.apache.hadoop.hdfs.protocol.HdfsConstants;

/**
  * Config constants.
*/
public class HDFSConstants extends HdfsConstants {

    public static final String HOSTS      = "Hosts";
    public static final String HOST       = "Host";
    public static final String PORTS      = "Ports";
    public static final String PORT       = "Port";

    public static final String CONFIG_CORE_SITE   = "core-site";
    public static final String CONFIG_HDFS_SITE   = "hdfs-site";
    public static final String CONFIG_MAPRED_SITE = "mapred-site";

    public static final String CONFIG_DIFERENT_PARTITIONS = "DiferentPartitions";
    //By default if not indicate the config in diferents partitions
    public static final String CONFIG_ONE_PARTITION       = "OnePartition";

    public static final String CONFIG_PARTITION_NAME      = "PartitionName";
    public static final String CONFIG_EXTENSION_NAME      = "ExtensionName";
}
