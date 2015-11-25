package com.gz.dt.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by naonao on 2015/10/27.
 */
public class JobUtils {

    public static void addFileToClassPath(Path file, Configuration conf, FileSystem fs) throws IOException {
        Configuration defaultConf = new Configuration();
        XConfiguration.copy(conf, defaultConf);
        if (fs == null) {
            // it fails with conf, therefore we pass defaultConf instead
            fs = file.getFileSystem(defaultConf);
        }
        // Hadoop 0.20/1.x.
        if (defaultConf.get("yarn.resourcemanager.webapp.address") == null) {
            // Duplicate hadoop 1.x code to workaround MAPREDUCE-2361 in Hadoop 0.20
            // Refer OOZIE-1806.
            String filepath = file.toUri().getPath();
            String classpath = conf.get("mapred.job.classpath.files");
            conf.set("mapred.job.classpath.files", classpath == null
                    ? filepath
                    : classpath + System.getProperty("path.separator") + filepath);
            URI uri = fs.makeQualified(file).toUri();
            DistributedCache.addCacheFile(uri, conf);
        }
        else { // Hadoop 0.23/2.x
            DistributedCache.addFileToClassPath(file, conf, fs);
        }
    }
}
