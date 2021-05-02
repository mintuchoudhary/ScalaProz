package com.m2.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;


public class CopyFilesUnixToHDFS {

    private static final String SEPARATOR = "/";
    private static final String PATH1="path1";
    private static final String PATH2 ="path2";
    private static final String FILE_SUFFIX =".gz";

    public static void main(String[] args) throws IOException{

        Configuration conf = new Configuration() ;

        if(args.length != 7){
            System.err.println("Usage: CopyFilesUnixToHDFS <date> <source systems> ...");
            System.exit(1);
        }

        String runDate = args[0];
        String runDateFormatted =args[1];
        String sourceSystem= args[2];
        String baseOutputPath = args[3];
        String unixInputPath=args[4];
        String path1Uncompressed=args[5];
        String path2Uncompressed=args[6];

        List<File> PATH1Files = getListOfFiles(runDate, unixInputPath.concat(SEPARATOR).concat(PATH1));
        List<File> PATH2Files = getListOfFiles(runDate, unixInputPath.concat(SEPARATOR).concat(PATH2));

        FileSystem fs = FileSystem.get(conf);

        String path1=baseOutputPath.concat(SEPARATOR).concat(sourceSystem).concat(SEPARATOR).concat
                (runDateFormatted).concat(SEPARATOR).concat(path1Uncompressed);
        String  path2=baseOutputPath.concat(SEPARATOR).concat(sourceSystem).concat(SEPARATOR).concat
                (runDateFormatted).concat(SEPARATOR).concat(path2Uncompressed);

        copyFilesTOHDFS(PATH1Files, path1, PATH1, conf, fs);
        copyFilesTOHDFS(PATH2Files, path2, PATH2, conf, fs);

    }

    private static void copyFilesTOHDFS(List<File> Files, String outputHDFSPath, String type,
                                        Configuration conf, FileSystem fs) throws IOException{

        if (Files != null && Files.size()>0) {


            for (int i = 0; i < Files.size(); i++) {
                OutputStream out = null;
                File file = Files.get(i);
                String filename = file.getName().substring(0, file.getName().lastIndexOf('.'));
                try {

                    Path outFile = new Path(outputHDFSPath.concat(SEPARATOR).concat(filename));
                    out = fs.create(outFile);
                    FileInputStream fis = new FileInputStream(file);
                    if (fis.read() != -1) {
                        GZIPInputStream gis = new GZIPInputStream(new FileInputStream(file));

                        byte[] buffer = new byte[8192];
                        int len;
                        while ((len = gis.read(buffer)) != -1) {
                            out.write(buffer, 0, len);
                        }
                        //fis.close();
                        gis.close();
                    }

                    if (out != null) {
                        //out.flush();
                        out.close();
                    }
                    file.delete();

                }catch (ZipException e) {
                    throw new IOException("Exception occured in " +type+ " while copying the file: " + filename + " with the exception: " + e);
                }catch (IOException e) {
                    throw new IOException("Exception occured in " +type+ " while copying the file: " + filename + " with the exception: " + e);
                }
            }
        }

    }
    private static List<File> getListOfFiles (String date, String unixPath){
        List<File> misFiles = new ArrayList<>();

        File[] files = new File(unixPath).listFiles();

        if( files != null){
            for(int i = 0; i<files.length; i++){
                String filename = files[i].getName();
                if(filename.contains(date) && filename.endsWith(FILE_SUFFIX)){
                    //misFiles.add(files[i].getAbsolutePath());
                    misFiles.add(files[i]);
                }

            }
        }
        return misFiles;
    }
}
