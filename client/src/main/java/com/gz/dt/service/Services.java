package com.gz.dt.service;

import com.gz.dt.client.SubmitClient;
import com.gz.dt.exception.ServiceException;
import com.gz.dt.util.*;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.VersionInfo;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by naonao on 2015/10/26.
 */
public class Services {
    private static final int MAX_SYSTEM_ID_LEN = 10;
    public static final String OOZIE_HOME_DIR = "oozie.home.dir";
    public static final String CONF_SYSTEM_ID = "oozie.system.id";
    public static final String CONF_SERVICE_CLASSES = "oozie.services";
    public static final String CONF_SERVICE_EXT_CLASSES = "oozie.services.ext";
    public static final String CONF_SYSTEM_MODE = "oozie.systemmode";
    public static final String CONF_DELETE_RUNTIME_DIR = "oozie.delete.runtime.dir.on.shutdown";
    private static Services SERVICES;

    private SubmitClient.SYSTEM_MODE systemMode;
    private String runtimeDir;
    private Configuration conf;
    private Map<Class<? extends Service>, Service> services = new LinkedHashMap<Class<? extends Service>, Service>();
    private String systemId;
    private static String oozieHome;


    public static void setOozieHome() throws ServiceException {
        //oozieHome = System.getProperty(OOZIE_HOME_DIR);
        oozieHome = SubmitClient.OOZIE_HOME;
        if (oozieHome == null) {
            throw new ServiceException(ErrorCode.E0000);
        }
        File file = new File(oozieHome);
        if (!file.isAbsolute()) {
            throw new ServiceException(ErrorCode.E0003, oozieHome);
        }
        if (!file.exists()) {
            throw new ServiceException(ErrorCode.E0004, oozieHome);
        }
    }

    public static String getOozieHome() throws ServiceException {
        return oozieHome;
    }


    public Services() throws ServiceException {
        setOozieHome();
        if (SERVICES != null) {
            XLog log = XLog.getLog(getClass());
            log.warn(XLog.OPS, "Previous services singleton active, destroying it");
            SERVICES.destroy();
            SERVICES = null;
        }
        setServiceInternal(XLogService.class, false);
        setServiceInternal(ConfigurationService.class, true);
        conf = get(ConfigurationService.class).getConf();
        DateUtils.setConf(conf);
        if (!DateUtils.getOozieProcessingTimeZone().equals(DateUtils.UTC)) {
            XLog.getLog(getClass()).warn("Oozie configured to work in a timezone other than UTC: {0}",
                    DateUtils.getOozieProcessingTimeZone().getID());
        }
        systemId = ConfigurationService.get(conf, CONF_SYSTEM_ID);
        if (systemId.length() > MAX_SYSTEM_ID_LEN) {
            systemId = systemId.substring(0, MAX_SYSTEM_ID_LEN);
            XLog.getLog(getClass()).warn("System ID [{0}] exceeds maximum length [{1}], trimming", systemId,
                    MAX_SYSTEM_ID_LEN);
        }
        setSystemMode(SubmitClient.SYSTEM_MODE.valueOf(ConfigurationService.get(conf, CONF_SYSTEM_MODE)));
        runtimeDir = createRuntimeDir();
    }


    public void init() throws ServiceException {
        XLog log = new XLog(LogFactory.getLog(getClass()));
        log.trace("Initializing");
        SERVICES = this;
        try {
            loadServices();
        }
        catch (RuntimeException rex) {
            XLog.getLog(getClass()).fatal(rex.getMessage(), rex);
            throw rex;
        }
        catch (ServiceException ex) {
            XLog.getLog(getClass()).fatal(ex.getMessage(), ex);
            SERVICES = null;
            throw ex;
        }
        InstrumentationService instrService = get(InstrumentationService.class);
        if (instrService != null) {
            Instrumentation instr = instrService.get();
            for (Service service : services.values()) {
                if (service instanceof Instrumentable) {
                    ((Instrumentable) service).instrument(instr);
                }
            }
            instr.addVariable("oozie", "version", new Instrumentation.Variable<String>() {

                public String getValue() {
                    return BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION);
                }
            });
            instr.addVariable("oozie", "mode", new Instrumentation.Variable<String>() {

                public String getValue() {
                    return getSystemMode().toString();
                }
            });
        }
        log.info("Initialized");
        log.info("Running with JARs for Hadoop version [{0}]", VersionInfo.getVersion());
        log.info("Oozie System ID [{0}] started!", getSystemId());
    }


    private String createRuntimeDir() throws ServiceException {
        try {
            File file = File.createTempFile(getSystemId(), ".dir");
            file.delete();
            if (!file.mkdir()) {
                ServiceException ex = new ServiceException(ErrorCode.E0001, file.getAbsolutePath());
                XLog.getLog(getClass()).fatal(ex);
                throw ex;
            }
            XLog.getLog(getClass()).info("Initialized runtime directory [{0}]", file.getAbsolutePath());
            return file.getAbsolutePath();
        }
        catch (IOException ex) {
            ServiceException sex = new ServiceException(ErrorCode.E0001, "", ex);
            XLog.getLog(getClass()).fatal(ex);
            throw sex;
        }
    }

    public String getSystemId() {
        return systemId;
    }

    public SubmitClient.SYSTEM_MODE getSystemMode() {
        return systemMode;
    }

    public String getRuntimeDir() {
        return runtimeDir;
    }

    public synchronized void setSystemMode(SubmitClient.SYSTEM_MODE sysMode) {
        if (this.systemMode != sysMode) {
            XLog log = XLog.getLog(getClass());
            log.info(XLog.OPS, "Exiting " + this.systemMode + " Entering " + sysMode);
        }
        this.systemMode = sysMode;
    }


    @Deprecated
    public Configuration getConf() {
        return conf;
    }



    public void setService(Class<? extends Service> klass) throws ServiceException {
        setServiceInternal(klass, true);
    }


    private void loadServices() throws ServiceException {
        XLog log = new XLog(LogFactory.getLog(getClass()));
        try {
            Map<Class, Service> map = new LinkedHashMap<Class, Service>();
            Class[] classes = ConfigurationService.getClasses(conf, CONF_SERVICE_CLASSES);
            log.debug("Services list obtained from property '" + CONF_SERVICE_CLASSES + "'");
            Class[] classesExt = ConfigurationService.getClasses(conf, CONF_SERVICE_EXT_CLASSES);
            log.debug("Services list obtained from property '" + CONF_SERVICE_EXT_CLASSES + "'");
            List<Service> list = new ArrayList<Service>();
            loadServices(classes, list);
            loadServices(classesExt, list);

            //removing duplicate services, strategy: last one wins
            for (Service service : list) {
                if (map.containsKey(service.getInterface())) {
                    log.debug("Replacing service [{0}] implementation [{1}]", service.getInterface(),
                            service.getClass());
                }
                map.put(service.getInterface(), service);
            }
            for (Map.Entry<Class, Service> entry : map.entrySet()) {
                setService(entry.getValue().getClass());
            }
        } catch (RuntimeException rex) {
            log.fatal("Runtime Exception during Services Load. Check your list of '" + CONF_SERVICE_CLASSES + "' or '" + CONF_SERVICE_EXT_CLASSES + "'");
            throw new ServiceException(ErrorCode.E0103, rex.getMessage(), rex);
        }
    }


    private void loadServices(Class[] classes, List<Service> list) throws ServiceException {
        XLog log = new XLog(LogFactory.getLog(getClass()));
        for (Class klass : classes) {
            try {
                Service service = (Service) klass.newInstance();
                log.debug("Loading service [{0}] implementation [{1}]", service.getInterface(),
                        service.getClass());
                if (!service.getInterface().isInstance(service)) {
                    throw new ServiceException(ErrorCode.E0101, klass, service.getInterface().getName());
                }
                list.add(service);
            } catch (ServiceException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new ServiceException(ErrorCode.E0102, klass, ex.getMessage(), ex);
            }
        }
    }


    private void setServiceInternal(Class<? extends Service> klass, boolean logging) throws ServiceException {
        try {
            Service newService = (Service) ReflectionUtils.newInstance(klass, null);
            Service oldService = services.get(newService.getInterface());
            if (oldService != null) {
                oldService.destroy();
            }
            if (logging) {
                XLog log = new XLog(LogFactory.getLog(getClass()));
                log.trace("Initializing service[{0}] class[{1}]", newService.getInterface(), newService.getClass());
            }
            newService.init(this);
            services.put(newService.getInterface(), newService);
        }
        catch (ServiceException ex) {
            XLog.getLog(getClass()).fatal(ex.getMessage(), ex);
            destroy();
            throw ex;
        }
    }


    public <T extends Service> T get(Class<T> serviceKlass) {
        return (T) services.get(serviceKlass);
    }


    public static Services get() {
        return SERVICES;
    }

    public void destroy() {
        XLog log = new XLog(LogFactory.getLog(getClass()));
        log.trace("Shutting down");
        boolean deleteRuntimeDir = false;
        if (conf != null) {
            deleteRuntimeDir = conf.getBoolean(CONF_DELETE_RUNTIME_DIR, false);
        }
        if (services != null) {
            List<Service> list = new ArrayList<Service>(services.values());
            Collections.reverse(list);
            for (Service service : list) {
                try {
                    log.trace("Destroying service[{0}]", service.getInterface());
                    if (service.getInterface() == XLogService.class) {
                        log.info("Shutdown");
                    }
                    service.destroy();
                }
                catch (Throwable ex) {
                    log.error("Error destroying service[{0}], {1}", service.getInterface(), ex.getMessage(), ex);
                }
            }
        }
        if (deleteRuntimeDir) {
            try {
                IOUtils.delete(new File(runtimeDir));
            }
            catch (IOException ex) {
                log.error("Error deleting runtime directory [{0}], {1}", runtimeDir, ex.getMessage(), ex);
            }
        }
        services = null;
        conf = null;
        SERVICES = null;
    }

}
