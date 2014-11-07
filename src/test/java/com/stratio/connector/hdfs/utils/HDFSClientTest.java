package com.stratio.connector.hdfs.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.hdfs.configuration.HDFSConstants;
import com.stratio.crossdata.common.exceptions.ExecutionException;

@RunWith(PowerMockRunner.class)
public class HDFSClientTest {

    private static final String HOST    = "127.0.0.1";
    private static final String PORT    = "9000";
    private static final String CATALOG = "catalog";
    private static final String TABLE   = "table";

    private static final String PROP_NAME   = "fs.default.name";

    private HDFSClient client;


    @Before
    public void before(){

        Configuration config = new Configuration();
        config.set(PROP_NAME, HDFSConstants.HDFS_URI_SCHEME+"://"+ HOST+":" +PORT);

        client = new HDFSClient(HOST,PORT);
    }

    @Test
    public void addFileTest() throws ExecutionException, IOException {

        client.addFile("songs.csv", "/user/hadoop/");

        client.readFile     ("/user/hadoop/logs/songs.csv");

        client.addRowToFile("211\tGreen Day\tHoliday\t2005\t5\tHoliday\n", "/user/hadoop/logs/songs.csv");

    }




}
