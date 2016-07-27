package com.epam.training.hadoop.hdfs;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class HdfsFileWriter implements AutoCloseable {

    BufferedWriter br;
    FileSystem hdfs;

    public HdfsFileWriter(Path file) throws IOException {

        hdfs = HdfsFileSystem.getInst();
        OutputStream os = hdfs.create(file);
        br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
    }

    public void writeLineToFile(String line) throws IOException {
        br.write(line);
        br.newLine();
    }

    @Override
    public void close() throws IOException {
        br.close();
    }

}