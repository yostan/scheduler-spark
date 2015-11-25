package com.gz.dt.util;

/**
 * Created by naonao on 2015/10/27.
 */
public abstract class FaultInjection {

    public static final String FAULT_INJECTION = "oozie.fault.injection";

    private static FaultInjection getFaultInjection(String className) {
        if (Boolean.parseBoolean(System.getProperty(FAULT_INJECTION, "false"))) {
            try {
                Class klass = Thread.currentThread().getContextClassLoader().loadClass(className);
                return (FaultInjection) klass.newInstance();
            }
            catch (ClassNotFoundException ex) {
                XLog.getLog(FaultInjection.class).warn("Trying to activate fault injection in production", ex);
            }
            catch (IllegalAccessException ex) {
                throw new RuntimeException(XLog.format("Could not initialize [{0}]", className, ex), ex);
            }
            catch (InstantiationException ex) {
                throw new RuntimeException(XLog.format("Could not initialize [{0}]", className, ex), ex);
            }
        }
        return null;
    }

    public static boolean activate(String className) {
        FaultInjection fi = getFaultInjection(className);
        if (fi != null) {
            className = className.substring(className.lastIndexOf(".") + 1);
            if (fi.activate()) {
                XLog.getLog(FaultInjection.class).warn("FAULT INJECTION, ACTIVATING [{0}]", className);
                return true;
            }
            else {
                XLog.getLog(FaultInjection.class).warn("FAULT INJECTION, DID NOT ACTIVATE [{0}]", className);
            }
        }
        return false;
    }

    public static void deactivate(String className) {
        FaultInjection fi = getFaultInjection(className);
        if (fi != null) {
            className = className.substring(className.lastIndexOf(".") + 1);
            if (fi.isActive()) {
                XLog.getLog(FaultInjection.class).warn("FAULT INJECTION, DEACTIVATING [{0}]", className);
                fi.deactivate();
            }
            else {
                XLog.getLog(FaultInjection.class).warn("FAULT INJECTION, CANNOT DEACTIVATE, NOT ACTIVE [{0}]",
                        className);
            }
        }
    }

    public static boolean isActive(String className) {
        FaultInjection fi = getFaultInjection(className);
        if (fi != null) {
            className = className.substring(className.lastIndexOf(".") + 1);
            if (fi.isActive()) {
                XLog.getLog(FaultInjection.class).warn("FAULT INJECTION, ACTIVE [{0}]", className);
                return true;
            }
            else {
                XLog.getLog(FaultInjection.class).warn("FAULT INJECTION, NOT ACTIVE [{0}]", className);
            }
        }
        return false;
    }

    public abstract boolean activate();

    public abstract void deactivate();

    public abstract boolean isActive();
}
