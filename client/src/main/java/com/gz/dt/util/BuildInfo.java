package com.gz.dt.util;

import java.util.Properties;

/**
 * Created by naonao on 2015/10/27.
 */
public class BuildInfo {

    public final static String BUILD_USER_NAME = "build.user";

    public final static String BUILD_TIME = "build.time";

    public final static String BUILD_VERSION = "build.version";

    public final static String BUILD_VC_REVISION = "vc.revision";

    public final static String BUILD_VC_URL = "vc.url";

    private static final Properties BUILD_INFO;

    static {
        BUILD_INFO = new Properties();
        try {
            BUILD_INFO.load(BuildInfo.class.getClassLoader().getResourceAsStream("oozie-buildinfo.properties"));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private BuildInfo() {
    }

    /**
     * Return the build info properties.
     *
     * @return the build info properties.
     */
    public static Properties getBuildInfo() {
        return BUILD_INFO;
    }
}
