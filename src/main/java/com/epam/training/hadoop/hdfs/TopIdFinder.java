package com.epam.training.hadoop.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TopIdFinder {

    private static FileSystem hdfs;
    private static LineAggregator result = new LineAggregator();
    private static int topCount;
    private static long totalLength;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Use reader <input path>");
            System.exit(-1);
        }

        try {

            Logger.log(getJvmOpts());
            hdfs = HdfsFileSystem.getInst();

            List<Path> inputFiles = getPathList(args[0]);
            Path outputFile = new Path(args[1]);
            topCount = Integer.parseInt(args[2]);

            long startReadTime = System.nanoTime();

            for (Path path : inputFiles) {
                parseFile(path);
            }

            Logger.logTimePassed("TOTAL TIME TO READ AND PARSE ALL FILES (" + totalLength / (1024 * 1024) + " Mb), sec: ",
                    startReadTime);

            long startCalcTime = System.nanoTime();

            writeTop(outputFile);

            Logger.logTimePassed("TOTAL TIME TO AGGREGATE AND WRITE TOP TO FILE, sec: ",
                    startCalcTime);

        } catch (IOException e) {
            Logger.log(e.getMessage());
        } finally {

            if (hdfs != null) {

                try {
                    Logger.writeStats(args[1]);
                    hdfs.close();
                } catch (IOException e) {
                    Logger.log(e.getMessage());
                }
            }
        }

    }

    private static void writeTop(Path outputFile) throws IOException {

        Logger.log("Calculating top and writing to file");
        Logger.logMemoryStatus();

        try (HdfsFileWriter writer = new HdfsFileWriter(outputFile)) {

            List<Map.Entry<Integer, String>> top = result.getTop(topCount);
            for (Map.Entry<Integer, String> entry : top) {
                writer.writeLineToFile(entry.getKey() + " : " + entry.getValue());
            }
        }

        Logger.logMemoryStatus();

    }


    private static void parseFile(Path path) {

        Logger.logMemoryStatus();

        try (HdfsFileReader reader = new HdfsFileReader(path)) {
            totalLength += hdfs.getFileStatus(path).getLen();
            String line = null;
            while ((line = reader.readLine()) != null) {
                result.addLine(line);
            }
        } catch (IOException e) {
            Logger.log(e.getMessage());
        }

        Logger.logMemoryStatus();
    }

    static List<Path> getPathList(String inputPath) throws FileNotFoundException, IOException {

        Path path = new Path(inputPath);

        List<Path> paths = new ArrayList<Path>();
        if (hdfs.exists(path)) {
            if (hdfs.isFile(path)) {
                paths.add(path);
            } else if (hdfs.isDirectory(path)) {
                for (FileStatus fStatus : hdfs.listStatus(path)) {
                    paths.add(fStatus.getPath());
                }
            }
        }

        return paths;
    }

    public static String getJvmOpts() {
        StringBuilder sb = new StringBuilder();


        for (String arg : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            sb.append("arg:").append(arg).append("|");
        }
        return sb.toString();
    }
}
