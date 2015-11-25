package com.gz.dt.service;

import com.gz.dt.exception.ServiceException;
import com.gz.dt.util.ErrorCode;
import com.gz.dt.util.Instrumentation;
import com.gz.dt.util.XLog;

import java.util.Map;

/**
 * Created by naonao on 2015/10/27.
 */
public class InstrumentationService implements Service {

    private static final String JVM_INSTRUMENTATION_GROUP = "jvm";

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "InstrumentationService.";

    public static final String CONF_LOGGING_INTERVAL = CONF_PREFIX + "logging.interval";

    private final XLog log = XLog.getLog(XLog.INSTRUMENTATION_LOG_NAME);

    protected static Instrumentation instrumentation = null;

    private static boolean isEnabled = false;




    public void init(Services services) throws ServiceException {
        final Instrumentation instr = new Instrumentation();
        int interval = ConfigurationService.getInt(services.getConf(), CONF_LOGGING_INTERVAL);  //default 60s
        initLogging(services, instr, interval);
        instr.addVariable(JVM_INSTRUMENTATION_GROUP, "free.memory", new Instrumentation.Variable<Long>() {

            public Long getValue() {
                return Runtime.getRuntime().freeMemory();
            }
        });
        instr.addVariable(JVM_INSTRUMENTATION_GROUP, "max.memory", new Instrumentation.Variable<Long>() {

            public Long getValue() {
                return Runtime.getRuntime().maxMemory();
            }
        });
        instr.addVariable(JVM_INSTRUMENTATION_GROUP, "total.memory", new Instrumentation.Variable<Long>() {

            public Long getValue() {
                return Runtime.getRuntime().totalMemory();
            }
        });
        instrumentation = instr;
        isEnabled = true;
    }


    protected void initLogging(Services services, final Instrumentation instr, int interval) throws ServiceException {
        log.info("*********** Startup ***********");
        log.info("Java System Properties: {E}{0}", mapToString(instr.getJavaSystemProperties()));
        log.info("OS Env: {E}{0}", mapToString(instr.getOSEnv()));
        SchedulerService schedulerService = services.get(SchedulerService.class);
        if (schedulerService != null) {
            instr.setScheduler(schedulerService.getScheduler());
            if (interval > 0) {
                Runnable instrumentationLogger = new Runnable() {

                    public void run() {
                        try {
                            log.info("\n" + instr.toString());
                        }
                        catch (Throwable ex) {
                            log.warn("Instrumentation logging error", ex);
                        }
                    }
                };
                schedulerService.schedule(instrumentationLogger, interval, interval, SchedulerService.Unit.SEC);
            }
        }
        else {
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), "SchedulerService unavailable");
        }
    }


    protected String mapToString(Map<String, String> map) {
        String E = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            sb.append("    ").append(entry.getKey()).append(" = ").append(entry.getValue()).append(E);
        }
        return sb.toString();
    }



    public void destroy() {
        isEnabled = false;
        instrumentation = null;
    }



    public Class<? extends Service> getInterface() {
        return InstrumentationService.class;
    }

    public Instrumentation get() {
        return instrumentation;
    }

    public static boolean isEnabled() {
        return isEnabled;
    }


}
