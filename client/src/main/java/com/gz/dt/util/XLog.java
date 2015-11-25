package com.gz.dt.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by naonao on 2015/10/26.
 */
public class XLog implements Log {

    public static final String INSTRUMENTATION_LOG_NAME = "oozieinstrumentation";

    public static final int STD = 1;
    public static final int OPS = 4;
    private static final int ALL = STD | OPS;
    private static final int[] LOGGER_MASKS = {STD, OPS};
    Log[] loggers;
    private String prefix = null;


    public static XLog getLog(String name) {
        return new XLog(LogFactory.getLog(name));
    }

    public static XLog getLog(Class clazz) {
        return new XLog(LogFactory.getLog(clazz));
    }

    public static XLog resetPrefix(XLog log) {
        log.setMsgPrefix(Info.get().createPrefix());
        return log;
    }

    public void setMsgPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getMsgPrefix() {
        return prefix;
    }

    public XLog(Log log) {
        loggers = new Log[2];
        loggers[0] = log;
        loggers[1] = LogFactory.getLog("oozieops");
    }


    private boolean isEnabled(Level level, int loggerMask) {
        for(int i=0; i < loggers.length; i++) {
            if((LOGGER_MASKS[i] & loggerMask) != 0) {
                boolean enabled = false;
                switch (level) {
                    case FATAL:
                        enabled = loggers[i].isFatalEnabled();
                        break;
                    case ERROR:
                        enabled = loggers[i].isErrorEnabled();
                        break;
                    case INFO:
                        enabled = loggers[i].isInfoEnabled();
                        break;
                    case WARN:
                        enabled = loggers[i].isWarnEnabled();
                        break;
                    case DEBUG:
                        enabled = loggers[i].isDebugEnabled();
                        break;
                    case TRACE:
                        enabled = loggers[i].isTraceEnabled();
                        break;
                }
                if (enabled) {
                    return true;
                }
            }
        }
        return false;
    }


    private void log(Level level, int loggerMask, String msgTemplate, Object... params) {
        loggerMask |= STD;
        if (isEnabled(level, loggerMask)) {
            String prefix = getMsgPrefix() != null ? getMsgPrefix() : Info.get().getPrefix();
            prefix = (prefix != null && prefix.length() > 0) ? prefix + " " : "";

            String msg = prefix + format(msgTemplate, params);
            Throwable throwable = getCause(params);

            for (int i = 0; i < LOGGER_MASKS.length; i++) {
                if (isEnabled(level, loggerMask & LOGGER_MASKS[i])) {
                    Log log = loggers[i];
                    switch (level) {
                        case FATAL:
                            log.fatal(msg, throwable);
                            break;
                        case ERROR:
                            log.error(msg, throwable);
                            break;
                        case INFO:
                            log.info(msg, throwable);
                            break;
                        case WARN:
                            log.warn(msg, throwable);
                            break;
                        case DEBUG:
                            log.debug(msg, throwable);
                            break;
                        case TRACE:
                            log.trace(msg, throwable);
                            break;
                    }
                }
            }
        }
    }


    public static Throwable getCause(Object... params) {
        Throwable throwable = null;
        if (params != null && params.length > 0 && params[params.length - 1] instanceof Throwable) {
            throwable = (Throwable) params[params.length - 1];
        }
        return throwable;
    }



    public boolean isDebugEnabled() {
        return isEnabled(Level.DEBUG, ALL);
    }

    public boolean isErrorEnabled() {
        return isEnabled(Level.ERROR, ALL);
    }

    public boolean isFatalEnabled() {
        return isEnabled(Level.FATAL, ALL);
    }

    public boolean isInfoEnabled() {
        return isEnabled(Level.INFO, ALL);
    }

    public boolean isTraceEnabled() {
        return isEnabled(Level.TRACE, ALL);
    }

    public boolean isWarnEnabled() {
        return isEnabled(Level.WARN, ALL);
    }

    public void trace(Object o) {
        log(Level.TRACE, STD, "{0}", o);
    }

    public void trace(Object o, Throwable throwable) {
        log(Level.TRACE, STD, "{0}", o, throwable);
    }

    public void debug(Object o) {
        log(Level.DEBUG, STD, "{0}", o);
    }

    public void debug(Object o, Throwable throwable) {
        log(Level.DEBUG, STD, "{0}", o, throwable);
    }

    public void info(Object o) {
        log(Level.INFO, STD, "{0}", o);
    }

    public void info(Object o, Throwable throwable) {
        log(Level.INFO, STD, "{0}", o, throwable);
    }

    public void warn(Object o) {
        log(Level.WARN, STD, "{0}", o);
    }

    public void warn(Object o, Throwable throwable) {
        log(Level.WARN, STD, "{0}", o, throwable);
    }

    public void error(Object o) {
        log(Level.ERROR, STD, "{0}", o);
    }

    public void error(Object o, Throwable throwable) {
        log(Level.ERROR, STD, "{0}", o, throwable);
    }

    public void fatal(Object o) {
        log(Level.FATAL, STD, "{0}", o);
    }

    public void fatal(Object o, Throwable throwable) {
        log(Level.FATAL, STD, "{0}", o, throwable);
    }



    public void fatal(String msgTemplate, Object... params) {
        log(Level.FATAL, STD, msgTemplate, params);
    }

    public void error(String msgTemplate, Object... params) {
        log(Level.ERROR, STD, msgTemplate, params);
    }

    public void info(String msgTemplate, Object... params) {
        log(Level.INFO, STD, msgTemplate, params);
    }

    public void warn(String msgTemplate, Object... params) {
        log(Level.WARN, STD, msgTemplate, params);
    }

    public void debug(String msgTemplate, Object... params) {
        log(Level.DEBUG, STD, msgTemplate, params);
    }

    public void trace(String msgTemplate, Object... params) {
        log(Level.TRACE, STD, msgTemplate, params);
    }


    public void fatal(int loggerMask, String msgTemplate, Object... params) {
        log(Level.FATAL, loggerMask, msgTemplate, params);
    }

    public void error(int loggerMask, String msgTemplate, Object... params) {
        log(Level.ERROR, loggerMask, msgTemplate, params);
    }

    public void info(int loggerMask, String msgTemplate, Object... params) {
        log(Level.INFO, loggerMask, msgTemplate, params);
    }

    public void warn(int loggerMask, String msgTemplate, Object... params) {
        log(Level.WARN, loggerMask, msgTemplate, params);
    }

    public void debug(int loggerMask, String msgTemplate, Object... params) {
        log(Level.DEBUG, loggerMask, msgTemplate, params);
    }

    public void trace(int loggerMask, String msgTemplate, Object... params) {
        log(Level.TRACE, loggerMask, msgTemplate, params);
    }



    public static String format(String msgTemplate, Object... params) {
        ParamChecker.notEmpty(msgTemplate, "msgTemplate");
        msgTemplate = msgTemplate.replace("{E}", System.getProperty("line.separator"));
        if (params != null && params.length > 0) {
            msgTemplate = MessageFormat.format(msgTemplate, params);
        }
        return msgTemplate;
    }


    public static class Info {
        private static String template = "";
        private String prefix = "";
        private static List<String> parameterNames = new ArrayList<String>();

        private static ThreadLocal<Info> tlLogInfo = new ThreadLocal<Info>() {
            @Override
            protected Info initialValue() {
                return new Info();
            }
        };


        public static void defineParameter(String name) {
            ParamChecker.notEmpty(name, "name");
            int count = parameterNames.size();
            if (count > 0) {
                template += " ";
            }
            template += name + "[{" + count + "}]";
            parameterNames.add(name);
        }

        public static void reset() {
            template = "";
            parameterNames.clear();
        }

        public static Info get() {
            return tlLogInfo.get();
        }

        public static void remove() {
            tlLogInfo.remove();
        }

        private Map<String, String> parameters = new HashMap<String, String>();

        public Info() {
        }

        public Info(Info logInfo) {
            setParameters(logInfo);
        }

        public void setParameters(Info logInfo) {
            parameters.clear();
            parameters.putAll(logInfo.parameters);
        }

        public void setParameter(String name, String value) {
            if (!parameterNames.contains(name)) {
                throw new IllegalArgumentException(format("Parameter[{0}] not defined", name));
            }
            parameters.put(name, value);
        }

        public String getParameter(String name) {
            return parameters.get(name);
        }

        public void clear() {
            parameters.clear();
            resetPrefix();
        }

        public void clearParameter(String name) {
            if (!parameterNames.contains(name)) {
                throw new IllegalArgumentException(format("Parameter[{0}] not defined", name));
            }
            parameters.remove(name);
        }

        public String createPrefix() {
            String[] params = new String[parameterNames.size()];
            for (int i = 0; i < params.length; i++) {
                params[i] = parameters.get(parameterNames.get(i));
                if (params[i] == null) {
                    params[i] = "-";
                }
            }
            return MessageFormat.format(template, (Object[]) params);
        }

        public String resetPrefix() {
            return prefix = createPrefix();
        }

        public String getPrefix() {
            return prefix;
        }
    }


    public enum Level {
        FATAL, ERROR, INFO, WARN, DEBUG, TRACE
    }


}
