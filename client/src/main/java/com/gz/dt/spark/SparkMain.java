package com.gz.dt.spark;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by naonao on 2015/10/21.
 */
public class SparkMain extends LauncherMain {

    private static final String CLASS_NAME_OPTION = "--class";
    private static final String MASTER_OPTION = "--master";
    private static final String MODE_OPTION = "--deploy-mode";
    private static final String JOB_NAME_OPTION = "--name";
    private static final String VERBOSE_OPTION = "--verbose";
    private static final String DELIM = " ";


    public static void main(String[] args) throws Exception {
        run(SparkMain.class, args);
    }


    @Override
    protected void run(String[] args) throws Exception {

        List<String> sparkArgs = new ArrayList<String>();


    }
}
