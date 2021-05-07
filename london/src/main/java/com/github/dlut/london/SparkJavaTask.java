package com.github.dlut.london;

import java.io.IOException;

public class SparkJavaTask {

    public static void preSpark(String fileName) {

    }

    public static void postJava(String fileName) {

    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Usage: <plugin(,plugin)*> <input file>");
            System.exit(1);
        }
        String pluginStr = args[0];
        String fileName = args[1];
//        int minPartition = Integer.parseInt(args[2]);
//        String alluxio = args[3];

        RheemTest.rheemTask(pluginStr, fileName);
    }
}
