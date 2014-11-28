package com.stratio.connector.hdfs.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.util.ConnectorParser;
import com.stratio.connector.hdfs.configuration.HDFSConstants;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;

public class HDFSClient {

    /**
     * The Log.
     */
    private static final Logger LOGGER      = LoggerFactory.getLogger(HDFSClient.class);

    private static final int    MAPSIZE     = 4 * 1024 ; // 4K - make this * 1024 to 4MB in a real system.
    private static final Short  replication = 1;
    private static final String PROP_NAME   = "fs.default.name";
    private static final String DEFAULT_EXTENSION =".csv";

    private static String separator   = ",";
    private static Boolean tableInDiferentPartitions = false;
    private static String  partitionName ="part";
    private static String  extension =".csv";

    private Configuration config = new Configuration();


    public HDFSClient(String host, String port) {

        config.set(PROP_NAME,HDFSConstants.HDFS_URI_SCHEME+"://"+ host+":" +port);
    }

    public HDFSClient(ConnectorClusterConfig clusterConfig) {

        Map<String, String> clusterOptions = clusterConfig.getClusterOptions();
        Map<String, String> values         = new HashMap<String, String>();

        //TODO: Recover the config from the clusterConfig ??
        //clusterOptions.get(HDFSConstants.CONFIG_CORE_SITE);
        //clusterOptions.get(HDFSConstants.CONFIG_HDFS_SITE);
        //clusterOptions.get(HDFSConstants.CONFIG_MAPRED_SITE);

        // Conf object will read the HDFS configuration parameters
        //config.addResource(new Path(HDFSConstants.CONFIG_CORE_SITE));

        if (clusterOptions.get(HDFSConstants.HOSTS) != null) {
            values.put(HDFSConstants.HOSTS, clusterOptions.get(HDFSConstants.HOSTS));
            String[] hosts = ConnectorParser.hosts(clusterOptions.get(HDFSConstants.HOSTS));

            values.put(HDFSConstants.HOST, hosts[0]);
        } else {
            values.put(HDFSConstants.HOST, clusterOptions.get(HDFSConstants.HOST));
        }

        if (clusterOptions.get(HDFSConstants.PORTS) != null) {
            values.put(HDFSConstants.PORTS, clusterOptions.get(HDFSConstants.PORTS));
            String[] ports = ConnectorParser.ports(clusterOptions.get(HDFSConstants.PORTS));

            values.put(HDFSConstants.PORT, ports[0]);
        } else {
            values.put(HDFSConstants.PORT, clusterOptions.get(HDFSConstants.PORT));
        }

        String h = values.get(HDFSConstants.HOST);
        String p = values.get(HDFSConstants.PORT);
        config.set(PROP_NAME, HDFSConstants.HDFS_URI_SCHEME + "://" + h + ":" + p);

        // Partitions in the table structure

        if(clusterOptions.get(HDFSConstants.CONFIG_PARTITIONS)!=null && clusterOptions.get(HDFSConstants
                .CONFIG_PARTITIONS).equals(HDFSConstants.CONFIG_DIFERENT_PARTITIONS)){
            tableInDiferentPartitions = true;
        }else if(clusterOptions.get(HDFSConstants.CONFIG_ONE_PARTITION)!=null && clusterOptions.get(HDFSConstants
                .CONFIG_PARTITIONS).equals(HDFSConstants.CONFIG_ONE_PARTITION)){
            tableInDiferentPartitions = false;
        }else{
            tableInDiferentPartitions = false;
        }

        if(clusterOptions.get(HDFSConstants.CONFIG_PARTITION_NAME)!=null){
            partitionName = clusterOptions.get(HDFSConstants.CONFIG_PARTITION_NAME);
        }

        if(clusterOptions.get(HDFSConstants.CONFIG_EXTENSION_NAME)!=null){
            extension = clusterOptions.get(HDFSConstants.CONFIG_EXTENSION_NAME);
        }

        if(clusterOptions.get(HDFSConstants.FILE_SEPARATOR)!=null){
            separator = clusterOptions.get(HDFSConstants.FILE_SEPARATOR);
        }

    }

    public HDFSClient( Configuration config) {
        this.config = config;
    }

    public String getseparator() {
        return separator;
    }

    public boolean ifExists (Path source) throws IOException{

        FileSystem hdfs = FileSystem.get(config);
        boolean isExists = hdfs.exists(source);
        return isExists;
    }

    public void getHostnames () throws IOException{

        FileSystem fs = FileSystem.get(config);
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

        String[] names = new String[dataNodeStats.length];
        for (int i = 0; i < dataNodeStats.length; i++) {
            names[i] = dataNodeStats[i].getHostName();
            LOGGER.info((dataNodeStats[i].getHostName()));
        }
    }

    public void getBlockLocations(String source) throws IOException{

        FileSystem fileSystem = FileSystem.get(config);
        Path srcPath = new Path(source);

        // Check if the file already exists
        if (!(ifExists(srcPath))) {
            LOGGER.info("No such destination " + srcPath);
            return;
        }
        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        FileStatus fileStatus = fileSystem.getFileStatus(srcPath);

        BlockLocation[] blkLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        int blkCount = blkLocations.length;

        LOGGER.info("File :" + filename + "stored at:");
        for (int i=0; i < blkCount; i++) {
            String[] hosts = blkLocations[i].getHosts();
            System.out.format("Host %d: %s %n", i, hosts);
        }

    }

    public void getModificationTime(String source) throws IOException{

        FileSystem fileSystem = FileSystem.get(config);
        Path srcPath = new Path(source);

        // Check if the file already exists
        if (!(fileSystem.exists(srcPath))) {
            LOGGER.info("No such destination " + srcPath);
            return;
        }
        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        FileStatus fileStatus = fileSystem.getFileStatus(srcPath);
        long modificationTime = fileStatus.getModificationTime();

        LOGGER.info("File %s; Modification time : %0.2f %n",filename,modificationTime);

    }

    public void copyFromLocal(String source, String dest) throws IOException {

        FileSystem fileSystem = FileSystem.get(config);
        Path srcPath = new Path(source);

        Path dstPath = new Path(dest);
        // Check if the file already exists
        if (!(fileSystem.exists(dstPath))) {
            LOGGER.info("No such destination " + dstPath);
            return;
        }

        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        try{
            fileSystem.copyFromLocalFile(srcPath, dstPath);
            LOGGER.info("File " + filename + "copied to " + dest);
        }catch(Exception e){
            LOGGER.error("Exception caught! :" + e);

        }finally{
            fileSystem.close();
        }
    }

    public void copyToLocal(String source, String dest) throws IOException {


        FileSystem fileSystem = FileSystem.get(config);
        Path srcPath = new Path(source);

        Path dstPath = new Path(dest);
        // Check if the file already exists
        if (!(fileSystem.exists(srcPath))) {
            LOGGER.info("No such destination " + srcPath);
            return;
        }

        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        try{
            fileSystem.copyToLocalFile(srcPath, dstPath);
            LOGGER.info("File " + filename + " copied to --> " + dest);
        }catch(Exception e){
            LOGGER.error("Exception caught! :" + e);

        }finally{
            fileSystem.close();
        }
    }

    public void renameFile(String fromthis, String tothis) throws IOException{

        FileSystem fileSystem = FileSystem.get(config);
        Path fromPath = new Path(fromthis);
        Path toPath = new Path(tothis);

        if (!(fileSystem.exists(fromPath))) {
            LOGGER.info("No such destination " + fromPath);
            return;
        }

        if (fileSystem.exists(toPath)) {
            LOGGER.info("Already exists! " + toPath);
            return;
        }

        try{
            boolean isRenamed = fileSystem.rename(fromPath, toPath);
            if(isRenamed){
                LOGGER.info("Renamed from " + fromthis + "to " + tothis);
            }
        }catch(Exception e){
            LOGGER.info("Exception :" + e);

        }finally{
            fileSystem.close();
        }

    }

    public void addFile(String source, String dest) throws ExecutionException {

        try {

            FileSystem fileSystem = FileSystem.get(config);

            // Get the filename out of the file path
            String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

            // Create the destination path including the filename.
            if (dest.charAt(dest.length() - 1) != '/') {
                dest = dest + "/" + filename;
            } else {
                dest = dest + filename;
            }

            // Check if the file already exists
            Path path = new Path(dest);
            if (fileSystem.exists(path)) {
                LOGGER.info("File " + dest + " already exists");
                return;
            }

            // Create a new file and write data to it.
            FSDataOutputStream out = fileSystem.create(path);

            InputStream in = new BufferedInputStream(new FileInputStream(
                    new File(source)));

            IOUtils.copyBytes(in, out, config);

            // Close all the file descripters
            in.close();
            out.close();
            fileSystem.close();

        }catch (IOException e){
            throw new ExecutionException("Exception "+e);
        }
    }

    public void addRowToFile(String source, String dest) throws ExecutionException {

        try {

            FileSystem fileSystem = FileSystem.get(config);

            if(tableInDiferentPartitions) {

                String filename = partitionName+extension;

                if (dest.charAt(dest.length() - 1) != '/') {
                    dest = dest + "/" + filename;
                } else {
                    dest = dest + filename;
                }
            }else{
                dest = dest + extension;
            }
            // Check if the file already exists
            Path path = new Path(dest);
            if (!fileSystem.exists(path)) {
                throw  new ExecutionException("File " + dest + " not exists");
            }

            // Create a new file and write data to it.
            FSDataOutputStream out = fileSystem.append(path);

            InputStream in = new BufferedInputStream(new ByteArrayInputStream(source.getBytes()));

            IOUtils.copyBytes(in, out, config);

            // Close all the file descripters
            in.close();
            out.close();
            fileSystem.close();

        }catch (IOException e){
            throw new ExecutionException("Exception "+e);
        }
    }

    public void addMetaDataToFile(String source, String dest) throws ExecutionException {

        try {

            FileSystem fileSystem = FileSystem.get(config);

            if(tableInDiferentPartitions) {

                String filename = partitionName+extension;

                if (dest.charAt(dest.length() - 1) != '/') {
                    dest = dest + "/" + filename;
                } else {
                    dest = dest + filename;
                }
            }else{
                dest = dest + extension;
            }
            // Check if the file already exists
            Path path = new Path(dest);
            if (!fileSystem.exists(path)) {
                throw  new ExecutionException("File " + dest + " not exists");
            }

            // Create a new file and write data to it.
            FSDataOutputStream out = fileSystem.append(path);

            InputStream in = new BufferedInputStream(new ByteArrayInputStream(source.getBytes()));

            IOUtils.copyBytes(in, out, config);

            // Close all the file descripters
            in.close();
            out.close();
            fileSystem.close();

        }catch (IOException e){
            throw new ExecutionException("Exception "+e);
        }
    }

    public void addFile(String dest) throws ExecutionException {

        try {

            FileSystem fileSystem = FileSystem.get(config);

            // Get the filename out of the file path

            String filename = dest.substring(dest.lastIndexOf('/') + 1, dest.length());

            // Create the destination path including the filename.
            if(tableInDiferentPartitions) {

                filename = partitionName+extension;

                if (dest.charAt(dest.length() - 1) != '/') {
                    dest = dest + "/" + filename;
                } else {
                    dest = dest + filename;
                }
            }else{
                dest = dest + extension;
            }
            // Check if the file already exists
            Path path = new Path(dest);
            if (fileSystem.exists(path)) {
                LOGGER.info("File " + dest + " already exists");
                return;
            }

            // Create a new file and write data to it.
            FSDataOutputStream out = fileSystem.create(path,replication);
            fileSystem.close();
        }catch (IOException e){
            throw new ExecutionException("Exception "+e);
        }
    }

    public void readFile(String file) throws ExecutionException {

        try {

            FileSystem fileSystem = FileSystem.get(config);

            Path path = new Path(file);
            if (!fileSystem.exists(path)) {
                LOGGER.info("File " + file + " does not exists");
                return;
            }

            FSDataInputStream in = fileSystem.open(path);

            String filename = file.substring(file.lastIndexOf('/') + 1,
                    file.length());

            OutputStream out = new BufferedOutputStream(new FileOutputStream(
                    new File(filename)));

            byte[] b = new byte[1024];
            int numBytes = 0;
            while ((numBytes = in.read(b)) > 0) {
                out.write(b, 0, numBytes);
            }

            in.close();
            out.close();
            fileSystem.close();
        }catch (IOException e){
            throw new ExecutionException("Exception "+e);
        }
    }

    public int searchInFile(String filePath, String inputSearch) throws ExecutionException, IOException {
        int count = 0,countBuffer=0,countLine=0;
        String lineNumber = "";
        BufferedReader br;
        String line = "";

        try {

            FileSystem fileSystem = FileSystem.get(config);

            Path path = new Path(filePath);
            if (!fileSystem.exists(path)) {
                LOGGER.info("File " + filePath + " does not exists");
                return 0;
            }

            FSDataInputStream in = fileSystem.open(path);

            br = new BufferedReader(new InputStreamReader(in));
            try {
                while((line = br.readLine()) != null)
                {
                    countLine++;

                    String[] words = line.split(separator);

                    for (String word : words) {
                        if (word.equals(inputSearch)) {
                            count++;
                            countBuffer++;
                        }
                    }

                    if(countBuffer > 0)
                    {
                        countBuffer = 0;
                        lineNumber += countLine + ",";
                    }

                }
                br.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                throw new ExecutionException("Exception "+e);
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            throw new ExecutionException("Exception "+e);
        }

        LOGGER.info("Times found at --" + count);
        LOGGER.info("Word found at  --" + lineNumber);

        return count;

    }


    private static String searchFor(String grepfor, java.nio.file.Path path) throws IOException {
        final byte[] tosearch = grepfor.getBytes(StandardCharsets.UTF_8);
        StringBuilder report = new StringBuilder();
        int padding = 1; // need to scan 1 character ahead in case it is a word boundary.
        int linecount = 0;
        int matches = 0;
        boolean inword = false;
        boolean scantolineend = false;
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);

        try {
            final long length = channel.size();
            int pos = 0;
            while (pos < length) {
                long remaining = length - pos;
                // int conversion is safe because of a safe MAPSIZE.. Assume a reaosnably sized tosearch.
                int trymap = MAPSIZE + tosearch.length + padding;
                int tomap = (int)Math.min(trymap, remaining);
                // different limits depending on whether we are the last mapped segment.
                int limit = trymap == tomap ? MAPSIZE : (tomap - tosearch.length);
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, pos, tomap);
                LOGGER.info("Mapped from " + pos + " for " + tomap);
                pos += (trymap == tomap) ? MAPSIZE : tomap;
                for (int i = 0; i < limit; i++) {
                    final byte b = buffer.get(i);
                    if (scantolineend) {
                        if (b == '\n') {
                            scantolineend = false;
                            inword = false;
                            linecount ++;
                        }
                    } else if (b == '\n') {
                        linecount++;
                        inword = false;
                    } else if (b == '\r' || b == ' ') {
                        inword = false;
                    } else if (!inword) {
                        if (wordMatch(buffer, i, tomap, tosearch)) {
                            matches++;
                            i += tosearch.length - 1;
                            if (report.length() > 0) {
                                report.append(", ");
                            }
                            report.append(linecount);
                            scantolineend = true;
                        } else {
                            inword = true;
                        }
                    }
                }
            }
        }finally {
            if(channel!=null){
                channel.close();
            }
        }
        return "Times found at--" + matches + "\nWord found at--" + report;
    }

    private static boolean wordMatch(MappedByteBuffer buffer, int pos, int tomap, byte[] tosearch) {
        //assume at valid word start.
        for (int i = 0; i < tosearch.length; i++) {
            if (tosearch[i] != buffer.get(pos + i)) {
                return false;
            }
        }
        byte nxt = (pos + tosearch.length) == tomap ? (byte)' ' : buffer.get(pos + tosearch.length);
        return nxt == ' ' || nxt == '\n' || nxt == '\r';
    }

    public void deleteFile(String dest) throws ExecutionException {

        try {

            FileSystem fileSystem = FileSystem.get(config);

            String filename = dest.substring(dest.lastIndexOf('/') + 1, dest.length());

            if(tableInDiferentPartitions) {

                filename = partitionName+extension;

                if (dest.charAt(dest.length() - 1) != '/') {
                    dest = dest + "/" + filename;
                } else {
                    dest = dest + filename;
                }
            }else{
                dest = dest + extension;
            }
            Path path = new Path(dest);
            if (!fileSystem.exists(path)) {
                LOGGER.info("File " + dest + " does not exists");
                return;
            }

            fileSystem.delete(new Path(dest), true);

            fileSystem.close();

        }catch (IOException e){
            throw new ExecutionException(" "+e);
        }

    }

    public void mkdir(String dir) throws ExecutionException {

        try{

            FileSystem fileSystem = FileSystem.get(config);

            Path path = new Path(dir);
            if (fileSystem.exists(path)) {
                LOGGER.info("Dir " + dir + " already exists!");
                return;
            }

            fileSystem.mkdirs(path);

            fileSystem.close();

        }catch (IOException e){
            throw new ExecutionException(" "+e);
        }

    }

    public void truncate(String file)throws ExecutionException{
       this.deleteFile(file);
        this.addFile(file);

    }

    public static void main(String[] args) throws IOException, ExecutionException {


        HDFSClient client = new HDFSClient("localhost","9000");


        client.searchInFile ("/user/hadoop/logs/1000songs.csv","Eminem");
        //client.searchFor ("Tony Bennett", new java.nio.file.FilePath() );
        //client.getHostnames();
        //client.mkdir  ("/user/hadoop/catalog");
        //client.readFile     ("/user/hadoop/logs/songs.csv");
        client.addFile("songs.csv", "/user/hadoop/");
        client.addRowToFile("211\tGreen Day\tHoliday\t2005\t5\tHoliday\n", "/user/hadoop/logs/songs.csv");

        LOGGER.info("Done!");
    }

    public static void supportedOperations(){
        LOGGER.info("Usage: hdfsclient add" + "<local_path> <hdfs_path>");
        LOGGER.info("Usage: hdfsclient read" + "<hdfs_path>");
        LOGGER.info("Usage: hdfsclient delete" + "<hdfs_path>");
        LOGGER.info("Usage: hdfsclient mkdir" + "<hdfs_path>");
        LOGGER.info("Usage: hdfsclient copyfromlocal" + "<local_path> <hdfs_path>");
        LOGGER.info("Usage: hdfsclient copytolocal" + " <hdfs_path> <local_path> ");
        LOGGER.info("Usage: hdfsclient modificationtime" + "<hdfs_path>");
        LOGGER.info("Usage: hdfsclient getblocklocations" + "<hdfs_path>");
        LOGGER.info("Usage: hdfsclient gethostnames");
    }

    public void createMetaDataFile(TableMetadata tableMetadata) throws ExecutionException {

        StringBuilder metaDataFile = new StringBuilder();

        metaDataFile.append("QualifiedName:"+tableMetadata.getName().getQualifiedName()+"\n");
        metaDataFile.append("CatalogName:"+tableMetadata.getName().getCatalogName().getName()+"\n");
        metaDataFile.append("TableName:"+tableMetadata.getName().getName()+"\n");
        metaDataFile.append("PrimaryKey:"+tableMetadata.getPrimaryKey().toString()+"\n");
        metaDataFile.append("Indexes:"+tableMetadata.getIndexes().toString()+"\n");
        Map<ColumnName,ColumnMetadata> columns = tableMetadata.getColumns();
        int i=0;
        for(ColumnName columnName: columns.keySet()){
            ColumnMetadata meta = columns.get(columnName);

            metaDataFile.append("Field"+(i++)+" -" +meta.getName().getName()+":"+meta.getColumnType().name()+"\n");
        }

        addFile(tableMetadata.getName().getCatalogName().getName()+"/."+
                tableMetadata.getName().getName());
        addMetaDataToFile(metaDataFile.toString(),tableMetadata.getName().getCatalogName().getName()+"/."+
                tableMetadata.getName().getName());
    }
}