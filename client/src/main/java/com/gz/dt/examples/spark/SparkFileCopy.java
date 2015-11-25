package com.gz.dt.examples.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by naonao on 2015/10/22.
 */
public class SparkFileCopy {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: SparkFileCopy <file> <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("SparkFileCopy");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0]);
        lines.saveAsTextFile(args[1]);
        System.out.println("Copied file from " + args[0] + " to " + args[1]);
        ctx.stop();

    }
}
