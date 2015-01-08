package com.stratio.connector.hdfs.configuration;

/**
  * Config constants.
*/
public class HDFSConstants  {

    public static final String HOSTS      = "Hosts";
    public static final String HOST       = "Host";
    public static final String PORTS      = "Ports";
    public static final String PORT       = "Port";

        //By default if not indicate the config is  one partition
    public static final String CONFIG_PARTITIONS          = "Partitions";
    public static final String CONFIG_ONE_PARTITION       = "OnePartition";
    public static final String CONFIG_DIFERENT_PARTITIONS = "DiferentPartitions";

    public static final String CONFIG_PARTITION_NAME      = "PartitionName";
    public static final String CONFIG_EXTENSION_NAME      = "Extension";

    public static final String FILE_SEPARATOR = "FileSeparator";

    /**
     * URI Scheme for hdfs://namenode/ URIs.
     */
    public static final String HDFS_URI_SCHEME = "hdfs";
}
