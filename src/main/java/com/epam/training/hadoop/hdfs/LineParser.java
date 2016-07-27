package com.epam.training.hadoop.hdfs;

public class LineParser {
    private static final String DELIMITER = "\t";
    private static final String NO_ID = "null";

    public static String parse(String line) {
        try {

            int idStart = line.indexOf(DELIMITER, line.indexOf(DELIMITER) + 1) + 1;
            int thirdTab = line.indexOf(DELIMITER, idStart);
            if (idStart == thirdTab)
                return NO_ID;

            return line.substring(idStart, thirdTab);

        } catch (StringIndexOutOfBoundsException ignore) {

            return "ERROR PARSING";
        }

    }
}
