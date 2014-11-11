package com.stratio.connector.hdfs.utils;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.crossdata.common.exceptions.ExecutionException;

@RunWith(PowerMockRunner.class)
public class HDFSClientTest {
    /**
     * The Log.
     */
    private static final Logger LOGGER      = LoggerFactory.getLogger(HDFSClientTest.class);

    private static final String HOST    = "127.0.0.1";
    private static final String PORT    = "9000";
    private static final String CATALOG = "catalog";
    private static final String TABLE   = "table";

    private static final String PROP_NAME   = "fs.default.name";
    private static final String SONGS_CSV   = "/user/hadoop/test/songs.csv";
    private static final String SONGS_NAME_CSV   = "songs.csv";

    private static final String SONGS_1000_CSV   = "/user/hadoop/test/1000songs.csv";
    private static final String AUTHOR_TO_SEARCH = "Eminem";
    private static final int    AUTHORS_FOUND_INTO_FILE = 4;
    private static final String SONGS_1000_NAME_CSV   = "1000songs.csv";

    private static final String TEST_MKDIR        = "/user/hadoop/test";
    private static final String TEST_DIR_SONGS    = "/user/hadoop/test/songs.csv";
    private static final String TEST_DIR_1000SONGS = "/user/hadoop/test/1000songs.csv";

    private HDFSClient client;


    @Before
    public void before(){

        Configuration config = new Configuration();
        //config.set(PROP_NAME, HDFSConstants.HDFS_URI_SCHEME+"://"+ HOST+":" +PORT);
        LOGGER.info(config.get(PROP_NAME));
        client = new HDFSClient(HOST,PORT);
    }

    @After
    public void afterClass() throws ExecutionException {

        //client.deleteFile(TEST_MKDIR);
    }

    @Test
    public void copyFileToHdfs() throws IOException, ExecutionException {

        client.mkdir  (TEST_MKDIR);
        client.addFile(TEST_DIR_SONGS);
        URL url = getClass().getClassLoader().getResource(SONGS_NAME_CSV);
        client.copyFromLocal(url.getFile(), TEST_DIR_SONGS);


    }

    @Test
    public void readFileAndCopyToLocalTest() throws ExecutionException, IOException {

        client.readFile(SONGS_CSV);
        File fileCreated = new File(SONGS_NAME_CSV);

        assertEquals(fileCreated.exists(),true);

        fileCreated.delete();

    }

    @Test
    public void searchInFileTest() throws ExecutionException, IOException {

        client.mkdir  (TEST_MKDIR);
        client.addFile(TEST_DIR_1000SONGS);
        URL url = getClass().getClassLoader().getResource(SONGS_1000_NAME_CSV);
        client.copyFromLocal(url.getFile(), TEST_DIR_1000SONGS);
        int result = client.searchInFile (SONGS_1000_CSV,AUTHOR_TO_SEARCH);

        assertEquals(result,AUTHORS_FOUND_INTO_FILE);

        File fileCreated = new File(SONGS_1000_NAME_CSV);
        fileCreated.delete();
    }

    @Test
    public void mkdirTest() throws ExecutionException, IOException {

        client.mkdir  (TEST_MKDIR);

    }


    @Test
    public void addFileTest() throws ExecutionException, IOException {

        //client.mkdir  ("/user/hadoop/catalog");
        client.addFile(SONGS_NAME_CSV, TEST_MKDIR);
        client.deleteFile(TEST_DIR_SONGS);

    }

    @Test
    public void addRowToFileTest() throws ExecutionException , IOException {

        client.addRowToFile("211\tGreen Day\tHoliday\t2005\t297\tHoliday\n", TEST_DIR_SONGS);


    }

    @Test
    public void deleteFileTest() throws ExecutionException {

        client.deleteFile(TEST_MKDIR);
        client.deleteFile(TEST_DIR_SONGS);
        client.deleteFile(TEST_DIR_1000SONGS);
    }
    @Test
    public void restTest() throws ExecutionException , IOException {

        client.addFile("/user/hadoop/logs/songs.csv");
        URL url = getClass().getClassLoader().getResource("songs.csv");
        client.copyFromLocal(url.getFile(), "/user/hadoop/logs/songs.csv");

        client.addRowToFile("211\tGreen Day\tHoliday\t2005\t297\tHoliday\n", "/user/hadoop/logs/songs.csv");

        client.addFile("songs.csv", "/user/hadoop/");

        client.addRowToFile("211\tGreen Day\tHoliday\t2005\t298\tHoliday\n", "/user/hadoop/logs/songs.csv");
        client.deleteFile("/user/hadoop/songs.csv");
        client.deleteFile("/user/hadoop/logs/songs.csv");

    }




}
