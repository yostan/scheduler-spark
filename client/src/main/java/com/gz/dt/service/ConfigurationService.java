package com.gz.dt.service;

import com.google.common.annotations.VisibleForTesting;
import com.gz.dt.exception.ServiceException;
import com.gz.dt.util.*;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Created by naonao on 2015/10/26.
 */
public class ConfigurationService implements Service, Instrumentable {

    private static final String INSTRUMENTATION_GROUP = "configuration";
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "ConfigurationService.";
    public static final String CONF_IGNORE_SYS_PROPS = CONF_PREFIX + "ignore.system.properties";
    public static final String CONF_VERIFY_AVAILABLE_PROPS = CONF_PREFIX + "verify.available.properties";
    public static final String OOZIE_CONFIG_DIR = "oozie.config.dir";
    public static final String OOZIE_DATA_DIR = "oozie.data.dir";
    public static final String OOZIE_CONFIG_FILE = "oozie.config.file";

    private static final Set<String> IGNORE_SYS_PROPS = new HashSet<String>();
    private static final Set<String> CONF_SYS_PROPS = new HashSet<String>();

    private static final String IGNORE_TEST_SYS_PROPS = "oozie.test.";
    private static final Set<String> MASK_PROPS = new HashSet<String>();
    private static Map<String,String> defaultConfigs = new HashMap<String,String>();

    private static Method getPasswordMethod;

    public static final String DEFAULT_CONFIG_FILE = "oozie-default.xml";
    public static final String SITE_CONFIG_FILE = "oozie-site.xml";

    private String configDir;     //OOZIE_HOME/conf
    private String configFile;    //OOZIE_HOME/conf/oozie-site.xml

    private LogChangesConfiguration configuration;


    static {

        //all this properties are seeded as system properties, no need to log changes
        IGNORE_SYS_PROPS.add(CONF_IGNORE_SYS_PROPS);
        IGNORE_SYS_PROPS.add(Services.OOZIE_HOME_DIR);
        IGNORE_SYS_PROPS.add(OOZIE_CONFIG_DIR);
        IGNORE_SYS_PROPS.add(OOZIE_CONFIG_FILE);
        IGNORE_SYS_PROPS.add(OOZIE_DATA_DIR);
        IGNORE_SYS_PROPS.add(XLogService.OOZIE_LOG_DIR);
        IGNORE_SYS_PROPS.add(XLogService.LOG4J_FILE);
        IGNORE_SYS_PROPS.add(XLogService.LOG4J_RELOAD);

        CONF_SYS_PROPS.add("oozie.http.hostname");
        CONF_SYS_PROPS.add("oozie.http.port");
//        CONF_SYS_PROPS.add(ZKUtils.OOZIE_INSTANCE_ID);

        // These properties should be masked when displayed because they contain sensitive info (e.g. password)
//        MASK_PROPS.add(JPAService.CONF_PASSWORD);
        MASK_PROPS.add("oozie.authentication.signature.secret");

        try {
            // Only supported in Hadoop 2.6.0+
            getPasswordMethod = Configuration.class.getMethod("getPassword", String.class);
        } catch (NoSuchMethodException e) {
            // Not supported
            getPasswordMethod = null;
        }
    }


    private static XLog log = XLog.getLog(ConfigurationService.class);


    public ConfigurationService() {
        log = XLog.getLog(ConfigurationService.class);
    }


    private InputStream getDefaultConfiguration() throws ServiceException, IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(DEFAULT_CONFIG_FILE);
        if (inputStream == null) {
            throw new ServiceException(ErrorCode.E0023, DEFAULT_CONFIG_FILE);
        }
        return inputStream;
    }


    private LogChangesConfiguration loadConf() throws ServiceException {
        XConfiguration configuration;
        try {
            InputStream inputStream = getDefaultConfiguration();
            configuration = loadConfig(inputStream, true);
            File file = new File(configFile);
            if (!file.exists()) {
                log.info("Missing site configuration file [{0}]", configFile);
            }
            else {
                inputStream = new FileInputStream(configFile);
                XConfiguration siteConfiguration = loadConfig(inputStream, false);
                XConfiguration.injectDefaults(configuration, siteConfiguration);
                configuration = siteConfiguration;
            }
        }
        catch (IOException ex) {
            throw new ServiceException(ErrorCode.E0024, configFile, ex.getMessage(), ex);
        }

        if (log.isTraceEnabled()) {
            try {
                StringWriter writer = new StringWriter();
                for (Map.Entry<String, String> entry : configuration) {
                    String value = getValue(configuration, entry.getKey());
                    writer.write(" " + entry.getKey() + " = " + value + "\n");
                }
                writer.close();
                log.trace("Configuration:\n{0}---", writer.toString());
            }
            catch (IOException ex) {
                throw new ServiceException(ErrorCode.E0025, ex.getMessage(), ex);
            }
        }

        String[] ignoreSysProps = configuration.getStrings(CONF_IGNORE_SYS_PROPS);
        if (ignoreSysProps != null) {
            IGNORE_SYS_PROPS.addAll(Arrays.asList(ignoreSysProps));
        }

        for (Map.Entry<String, String> entry : configuration) {
            String sysValue = System.getProperty(entry.getKey());
            if (sysValue != null && !IGNORE_SYS_PROPS.contains(entry.getKey())) {
                log.info("Configuration change via System Property, [{0}]=[{1}]", entry.getKey(), sysValue);
                configuration.set(entry.getKey(), sysValue);
            }
        }
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            String name = (String) entry.getKey();
            if (!IGNORE_SYS_PROPS.contains(name)) {
                if (name.startsWith("oozie.") && !name.startsWith(IGNORE_TEST_SYS_PROPS)) {
                    if (configuration.get(name) == null) {
                        log.warn("System property [{0}] no defined in Oozie configuration, ignored", name);
                    }
                }
            }
        }

        //Backward compatible, we should still support -Dparam.
        for (String key : CONF_SYS_PROPS) {
            String sysValue = System.getProperty(key);
            if (sysValue != null && !IGNORE_SYS_PROPS.contains(key)) {
                log.info("Overriding configuration with system property. Key [{0}], Value [{1}] ", key, sysValue);
                configuration.set(key, sysValue);
            }
        }

        return new LogChangesConfiguration(configuration);
    }


    private XConfiguration loadConfig(InputStream inputStream, boolean defaultConfig) throws IOException, ServiceException {
        XConfiguration configuration;
        configuration = new XConfiguration(inputStream);
        for(Map.Entry<String,String> entry: configuration) {
            if (defaultConfig) {
                defaultConfigs.put(entry.getKey(), entry.getValue());
            }
            else {
                log.debug("Overriding configuration with oozie-site, [{0}]", entry.getKey());
            }
        }
        return configuration;
    }


    public Configuration getConf() {
        if (configuration == null) {
            throw new IllegalStateException("Not initialized");
        }
        return configuration;
    }


    public String getConfigDir() {
        return configDir;
    }


    public Configuration getMaskedConfiguration() {
        XConfiguration maskedConf = new XConfiguration();
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
            String name = entry.getKey();
            String value = getValue(conf, name);
            maskedConf.set(name, value);
        }
        return maskedConf;
    }


    private String getValue(Configuration config, String key) {
        String value;
        if (MASK_PROPS.contains(key)) {
            value = "**MASKED**";
        }
        else {
            value = config.get(key);
        }
        return value;
    }

    @VisibleForTesting
    public static void set(String name, String value) {
        Configuration conf = Services.get().getConf();
        conf.set(name, value);
    }

    @VisibleForTesting
    public static void setBoolean(String name, boolean value) {
        Configuration conf = Services.get().getConf();
        conf.setBoolean(name, value);
    }

    public static String get(String name) {
        Configuration conf = Services.get().getConf();
        return get(conf, name);
    }

    public static String get(Configuration conf, String name) {
        return conf.get(name, "");
    }

    public static String[] getStrings(String name) {
        Configuration conf = Services.get().getConf();
        return getStrings(conf, name);
    }

    public static String[] getStrings(Configuration conf, String name) {
        return conf.getStrings(name, new String[0]);
    }

    public static boolean getBoolean(String name) {
        Configuration conf = Services.get().getConf();
        return getBoolean(conf, name);
    }

    public static boolean getBoolean(Configuration conf, String name) {
        return conf.getBoolean(name, false);
    }

    public static int getInt(String name) {
        Configuration conf = Services.get().getConf();
        return getInt(conf, name);
    }

    public static int getInt(Configuration conf, String name) {
        return conf.getInt(name, 0);
    }

    public static float getFloat(String name) {
        Configuration conf = Services.get().getConf();
        return conf.getFloat(name, 0f);
    }

    public static long getLong(String name) {
        Configuration conf = Services.get().getConf();
        return getLong(conf, name);
    }

    public static long getLong(Configuration conf, String name) {
        return conf.getLong(name, 0L);
    }

    public static Class<?>[] getClasses(String name) {
        Configuration conf = Services.get().getConf();
        return getClasses(conf, name);
    }

    public static Class<?>[] getClasses(Configuration conf, String name) {
        return conf.getClasses(name);
    }

    public static Class<?> getClass(Configuration conf, String name) {
        return conf.getClass(name, Object.class);
    }


    public static String getPassword(Configuration conf, String name) {
        if (getPasswordMethod != null) {
            try {
                char[] pass = (char[]) getPasswordMethod.invoke(conf, name);
                return new String(pass);
            } catch (IllegalAccessException e) {
                log.error(e);
                throw new IllegalArgumentException("Could not load password for [" + name + "]", e);
            } catch (InvocationTargetException e) {
                log.error(e);
                throw new IllegalArgumentException("Could not load password for [" + name + "]", e);
            }
        } else {
            return conf.get(name);
        }
    }


    public static String getPassword(String name) {
        Configuration conf = Services.get().getConf();
        return getPassword(conf, name);
    }



    public static String getConfigurationDirectory() throws ServiceException {
        String oozieHome = Services.getOozieHome();
        String configDir = System.getProperty(OOZIE_CONFIG_DIR, null);
        File file = configDir == null
                ? new File(oozieHome, "conf")
                : new File(configDir);
        if (!file.exists()) {
            throw new ServiceException(ErrorCode.E0024, configDir);
        }
        return file.getPath();
    }



    public void instrument(Instrumentation instr) {

        instr.addVariable(INSTRUMENTATION_GROUP, "config.dir", new Instrumentation.Variable<String>() {
            public String getValue() {
                return configDir;
            }
        });
        instr.addVariable(INSTRUMENTATION_GROUP, "config.file", new Instrumentation.Variable<String>() {
            public String getValue() {
                return configFile;
            }
        });

    }

    public void init(Services services) throws ServiceException {
        configDir = getConfigurationDirectory();
        configFile = System.getProperty(OOZIE_CONFIG_FILE, SITE_CONFIG_FILE);
        if (configFile.contains("/")) {
            throw new ServiceException(ErrorCode.E0022, configFile);
        }
        log.info("Oozie home dir  [{0}]", Services.getOozieHome());
        log.info("Oozie conf dir  [{0}]", configDir);
        log.info("Oozie conf file [{0}]", configFile);
        configFile = new File(configDir, configFile).toString();
        configuration = loadConf();
        if (configuration.getBoolean(CONF_VERIFY_AVAILABLE_PROPS, false)) {
            verifyConfigurationName();
        }
    }

    public void verifyConfigurationName() {
        for (Map.Entry<String, String> entry: configuration) {
            if (getDefaultOozieConfig(entry.getKey()) == null) {
                log.warn("Invalid configuration defined, [{0}] ", entry.getKey());
            }
        }
    }

    private String getDefaultOozieConfig(String name) {
        return defaultConfigs.get(name);
    }

    public void destroy() {
        configuration = null;

    }

    public Class<? extends Service> getInterface() {
        return ConfigurationService.class;
    }


    private class LogChangesConfiguration extends XConfiguration {

        public LogChangesConfiguration(Configuration conf) {
            for (Map.Entry<String, String> entry : conf) {
                if (get(entry.getKey()) == null) {
                    setValue(entry.getKey(), entry.getValue());
                }
            }
        }

        public String[] getStrings(String name) {
            String s = get(name);
            return (s != null && s.trim().length() > 0) ? super.getStrings(name) : new String[0];
        }

        public String[] getStrings(String name, String[] defaultValue) {
            String s = get(name);
            if (s == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name,
                        Arrays.asList(defaultValue).toString());
            }
            return (s != null && s.trim().length() > 0) ? super.getStrings(name) : defaultValue;
        }

        public String get(String name, String defaultValue) {
            String value = get(name);
            if (value == null) {
                boolean maskValue = MASK_PROPS.contains(name);
                value = defaultValue;
                String logValue = (maskValue) ? "**MASKED**" : defaultValue;
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, logValue);
            }
            return value;
        }

        public void set(String name, String value) {
            setValue(name, value);
            boolean maskValue = MASK_PROPS.contains(name);
            value = (maskValue) ? "**MASKED**" : value;
            log.info(XLog.OPS, "Programmatic configuration change, property[{0}]=[{1}]", name, value);
        }

        public boolean getBoolean(String name, boolean defaultValue) {
            String value = get(name);
            if (value == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, defaultValue);
            }
            return super.getBoolean(name, defaultValue);
        }

        public int getInt(String name, int defaultValue) {
            String value = get(name);
            if (value == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, defaultValue);
            }
            return super.getInt(name, defaultValue);
        }

        public long getLong(String name, long defaultValue) {
            String value = get(name);
            if (value == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, defaultValue);
            }
            return super.getLong(name, defaultValue);
        }

        public float getFloat(String name, float defaultValue) {
            String value = get(name);
            if (value == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, defaultValue);
            }
            return super.getFloat(name, defaultValue);
        }

        public Class<?>[] getClasses(String name, Class<?> ... defaultValue) {
            String value = get(name);
            if (value == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, defaultValue);
            }
            return super.getClasses(name, defaultValue);
        }

        public Class<?> getClass(String name, Class<?> defaultValue) {
            String value = get(name);
            if (value == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, defaultValue);
                return defaultValue;
            }
            try {
                return getClassByName(value);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        private void setValue(String name, String value) {
            super.set(name, value);
        }

    }

}
