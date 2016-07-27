package com.epam.training.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsFileSystem {

    private static FileSystem hdfs;

    public static FileSystem getInst() throws IOException {

        if (hdfs == null) {
            hdfs = FileSystem.get(getHdfsConfig());
            Logger.log("HDFS configured successfully");
        }
        return hdfs;
    }

    private static Configuration getHdfsConfig() {

        Configuration conf = new Configuration(true);
        conf.addResource(new Path("/etc/hadoop/2.4.0.0-169/0/hdfs-site.xml"));
        conf.addResource(new Path("/etc/hadoop/2.4.0.0-169/0/core-site.xml"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        return conf;
    }
}
