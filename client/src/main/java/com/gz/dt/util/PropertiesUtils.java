package com.gz.dt.util;

import com.gz.dt.exception.CommandException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;
import java.util.Set;

/**
 * Created by naonao on 2015/10/27.
 */
public class PropertiesUtils {

    public static final String HADOOP_USER = "user.name";
    public static final String YEAR = "YEAR";
    public static final String MONTH = "MONTH";
    public static final String DAY = "DAY";
    public static final String HOUR = "HOUR";
    public static final String MINUTE = "MINUTE";
    public static final String DAYS = "DAYS";
    public static final String HOURS = "HOURS";
    public static final String MINUTES = "MINUTES";
    public static final String KB = "KB";
    public static final String MB = "MB";
    public static final String GB = "GB";
    public static final String TB = "TB";
    public static final String PB = "PB";
    public static final String RECORDS = "RECORDS";
    public static final String MAP_IN = "MAP_IN";
    public static final String MAP_OUT = "MAP_OUT";
    public static final String REDUCE_IN = "REDUCE_IN";
    public static final String REDUCE_OUT = "REDUCE_OUT";
    public static final String GROUPS = "GROUPS";

    public static String propertiesToString(Properties props) {
        ParamChecker.notNull(props, "props");
        try {
            StringWriter sw = new StringWriter();
            props.store(sw, "");
            sw.close();
            return sw.toString();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Properties stringToProperties(String str) {
        ParamChecker.notNull(str, "str");
        try {
            StringReader sr = new StringReader(str);
            Properties props = new Properties();
            props.load(sr);
            sr.close();
            return props;
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Properties readProperties(Reader reader, int maxDataLen) throws IOException {
        String data = IOUtils.getReaderAsString(reader, maxDataLen);
        return stringToProperties(data);
    }

    /**
     * Create a set from an array
     *
     * @param properties String array
     * @param set String set
     */
    public static void createPropertySet(String[] properties, Set<String> set) {
        ParamChecker.notNull(set, "set");
        for (String p : properties) {
            set.add(p);
        }
    }

    /**
     * Validate against DISALLOWED Properties.
     *
     * @param conf : configuration to check.
     * @throws CommandException
     */
    public static void checkDisallowedProperties(Configuration conf, Set<String> set) throws CommandException {
        ParamChecker.notNull(conf, "conf");
        for (String prop : set) {
            if (conf.get(prop) != null) {
                throw new CommandException(ErrorCode.E0808, prop);
            }
        }
    }
}
