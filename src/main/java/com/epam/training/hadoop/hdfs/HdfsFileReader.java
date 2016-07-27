package com.epam.training.hadoop.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HdfsFileReader implements AutoCloseable {

    private FileSystem hdfs;
    private BufferedReader br;


    public HdfsFileReader(Path file) throws IOException {

        hdfs = HdfsFileSystem.getInst();

        if (hdfs.exists(file) && hdfs.isFile(file)) {
            br = new BufferedReader(new InputStreamReader(hdfs.open(file)));
        } else {
            throw new IOException("File not found: " + file.toString());
        }

    }

    public String readLine() throws IOException {
        return br.readLine();
    }

    public void close() throws IOException {
        br.close();
    }


}
