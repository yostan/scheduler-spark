package com.gz.dt;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by naonao on 2015/11/2.
 */
public class SparkFileCopy {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SparkFileCopy <file> <file>");
            System.exit(1);
        }

        System.out.println("args size: "+args.length);
        System.out.println("args[0]: "+args[0]);
        System.out.println("args[1]: "+args[1]);

        SparkConf sparkConf = new SparkConf().setAppName("SparkFileCopy").setMaster("spark://112.74.66.37:7077");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0]);
        lines.saveAsTextFile(args[1]);
        System.out.println("Copied file from " + args[0] + " to " + args[1]);
        ctx.stop();

    }
}
