package com.gz.dt.service;

import com.gz.dt.exception.ServiceException;
import com.gz.dt.util.*;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

/**
 * Created by naonao on 2015/10/26.
 */
public class XLogService implements Service, Instrumentable {

    private static final String INSTRUMENTATION_GROUP = "logging";
    public static final String OOZIE_LOG_DIR = "oozie.log.dir";
    public static final String LOG4J_FILE = "oozie.log4j.file";
    public static final String LOG4J_RELOAD = "oozie.log4j.reload";
    public static final String DEFAULT_LOG4J_PROPERTIES = "oozie-log4j.properties";
    public static final String DEFAULT_RELOAD_INTERVAL = "10";

    public static final String USER = "USER";
    public static final String GROUP = "GROUP";

    private XLog log;
    private long interval;
    private boolean fromClasspath;
    private String log4jFileName;
    private boolean logOverWS = true;
    private boolean errorLogEnabled = true;
    private boolean auditLogEnabled = true;

    private static final String STARTUP_MESSAGE = "{E}"
            + " ******************************************************************************* {E}"
            + "  STARTUP MSG: Oozie BUILD_VERSION [{0}] compiled by [{1}] on [{2}]{E}"
            + "  STARTUP MSG:       revision [{3}]@[{4}]{E}"
            + "*******************************************************************************";

    private String oozieLogPath;
    private String oozieLogName;
    private String oozieErrorLogPath;
    private String oozieErrorLogName;
    private String oozieAuditLogPath;
    private String oozieAuditLogName;
    private int oozieLogRotation = -1;
    private int oozieErrorLogRotation = -1;
    private int oozieAuditLogRotation = -1;

    public XLogService() {
    }

    public String getOozieLogPath() {
        return oozieLogPath;
    }

    public String getOozieErrorLogPath() {
        return oozieErrorLogPath;
    }

    public String getOozieLogName() {
        return oozieLogName;
    }

    public String getOozieErrorLogName() {
        return oozieErrorLogName;
    }


    public void init(Services services) throws ServiceException {
        String oozieHome = Services.getOozieHome();
        String oozieLogs = System.getProperty(OOZIE_LOG_DIR, oozieHome + "/logs");
        System.setProperty(OOZIE_LOG_DIR, oozieLogs);
        try {
            LogManager.resetConfiguration();
            log4jFileName = System.getProperty(LOG4J_FILE, DEFAULT_LOG4J_PROPERTIES);
            if (log4jFileName.contains("/")) {
                throw new ServiceException(ErrorCode.E0011, log4jFileName);
            }
            if (!log4jFileName.endsWith(".properties")) {
                throw new ServiceException(ErrorCode.E0012, log4jFileName);
            }
            String configPath = ConfigurationService.getConfigurationDirectory();
            File log4jFile = new File(configPath, log4jFileName);
            if (log4jFile.exists()) {
                fromClasspath = false;
            }
            else {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                URL log4jUrl = cl.getResource(log4jFileName);
                if (log4jUrl == null) {
                    throw new ServiceException(ErrorCode.E0013, log4jFileName, configPath);
                }
                fromClasspath = true;
            }

            if (fromClasspath) {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                URL log4jUrl = cl.getResource(log4jFileName);
                PropertyConfigurator.configure(log4jUrl);
            }
            else {
                interval = Long.parseLong(System.getProperty(LOG4J_RELOAD, DEFAULT_RELOAD_INTERVAL));
                PropertyConfigurator.configureAndWatch(log4jFile.toString(), interval * 1000);
            }

            log = new XLog(LogFactory.getLog(getClass()));

//            log.info(XLog.OPS, STARTUP_MESSAGE, BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION),
//                    BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_USER_NAME), BuildInfo.getBuildInfo()
//                            .getProperty(BuildInfo.BUILD_TIME), BuildInfo.getBuildInfo().getProperty(
//                            BuildInfo.BUILD_VC_REVISION), BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VC_URL));

            String from = (fromClasspath) ? "CLASSPATH" : configPath;
            String reload = (fromClasspath) ? "disabled" : Long.toString(interval) + " sec";
            log.info("Log4j configuration file [{0}]", log4jFileName);
            log.info("Log4j configuration file loaded from [{0}]", from);
            log.info("Log4j reload interval [{0}]", reload);

            XLog.Info.reset();
            XLog.Info.defineParameter(USER);
            XLog.Info.defineParameter(GROUP);
//            XLogFilter.reset();
//            XLogFilter.defineParameter(USER);
//            XLogFilter.defineParameter(GROUP);

            // Getting configuration for oozie log via WS
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            InputStream is = (fromClasspath) ? cl.getResourceAsStream(log4jFileName) : new FileInputStream(log4jFile);
            extractInfoForLogWebService(is);
        }
        catch (IOException ex) {
            throw new ServiceException(ErrorCode.E0010, ex.getMessage(), ex);
        }
    }


    private void extractInfoForLogWebService(InputStream is) throws IOException {
        Properties props = new Properties();
        props.load(is);

        Configuration conf = new XConfiguration();
        for (Map.Entry entry : props.entrySet()) {
            conf.set((String) entry.getKey(), (String) entry.getValue());
        }

//        XLogUtil logUtil = new XLogUtil(conf, "oozie");
//        logOverWS = logUtil.isLogOverEnable();
//        oozieLogRotation = logUtil.getLogRotation() == 0 ? oozieLogRotation : logUtil.getLogRotation();
//        oozieLogPath = logUtil.getLogPath() == null ? oozieLogPath : logUtil.getLogPath();
//        oozieLogName = logUtil.getLogFileName() == null ? oozieLogName : logUtil.getLogFileName();
//
//        logUtil = new XLogUtil(conf, "oozieError");
//        errorLogEnabled = logUtil.isLogOverEnable();
//        oozieErrorLogRotation = logUtil.getLogRotation() == 0 ? oozieErrorLogRotation : logUtil.getLogRotation();
//        oozieErrorLogPath = logUtil.getLogPath() == null ? oozieErrorLogPath : logUtil.getLogPath();
//        oozieErrorLogName = logUtil.getLogFileName() == null ? oozieErrorLogName : logUtil.getLogFileName();
//
//        logUtil = new XLogUtil(conf, "oozieaudit");
//        auditLogEnabled = logUtil.isLogOverEnable();
//        oozieAuditLogRotation = logUtil.getLogRotation() == 0 ? oozieAuditLogRotation : logUtil.getLogRotation();
//        oozieAuditLogPath = logUtil.getLogPath() == null ? oozieAuditLogPath : logUtil.getLogPath();
//        oozieAuditLogName = logUtil.getLogFileName() == null ? oozieAuditLogName : logUtil.getLogFileName();

    }









    public void instrument(Instrumentation instr) {
        instr.addVariable(INSTRUMENTATION_GROUP, "config.file", new Instrumentation.Variable<String>() {
            public String getValue() {
                return log4jFileName;
            }
        });
        instr.addVariable(INSTRUMENTATION_GROUP, "reload.interval", new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return interval;
            }
        });
        instr.addVariable(INSTRUMENTATION_GROUP, "from.classpath", new Instrumentation.Variable<Boolean>() {
            public Boolean getValue() {
                return fromClasspath;
            }
        });
        instr.addVariable(INSTRUMENTATION_GROUP, "log.over.web-service", new Instrumentation.Variable<Boolean>() {
            public Boolean getValue() {
                return logOverWS;
            }
        });

    }


    public void destroy() {
        LogManager.shutdown();
        XLog.Info.reset();
//        XLogFilter.reset();

    }

    public Class<? extends Service> getInterface() {
        return XLogService.class;
    }


    boolean getLogOverWS() {
        return logOverWS;
    }

    boolean isErrorLogEnabled(){
        return errorLogEnabled;
    }

    int getOozieLogRotation() {
        return oozieLogRotation;
    }

    int getOozieErrorLogRotation() {
        return oozieErrorLogRotation;
    }

    int getOozieAuditLogRotation() {
        return oozieAuditLogRotation;
    }

    public String getOozieAuditLogPath() {
        return oozieAuditLogPath;
    }

    public String getOozieAuditLogName() {
        return oozieAuditLogName;
    }

    boolean isAuditLogEnabled() {
        return auditLogEnabled;
    }

    String getLog4jProperties() {
        return log4jFileName;
    }

    boolean getFromClasspath() {
        return fromClasspath;
    }

}
