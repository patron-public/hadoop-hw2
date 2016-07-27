package com.epam.training.hadoop.hdfs;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Logger {

    private static List<String> detailedLog = new ArrayList<String>();

    public static void log(String msg) {
        detailedLog.add(msg);
        System.out.println(msg);
    }

    public static void logMemoryStatus() {
        Runtime rnt = Runtime.getRuntime();
        long total = rnt.totalMemory() / (1024 * 1024);
        long free = rnt.freeMemory() / (1024 * 1024);
        long used = total - free;
        log("Total/Used/Free Memory: " + total + " / " + used + " / " + free + " Mb");

    }

    public static void logTimePassed(String text, long startTime) {
        long timePassed = System.nanoTime() - startTime;
        log(text + (double) timePassed / 1000000000.0);
    }

    public static void writeStats(String outputFile) throws IOException {

        Path logFile = new Path(outputFile + ".detailed.log");
        try (HdfsFileWriter writer = new HdfsFileWriter(logFile)) {

            for (String msg : detailedLog) {
                writer.writeLineToFile(msg);
            }
        }
    }

}
