package com.gz.dt.executor;

import com.gz.dt.exception.ActionExecutorException;
import com.gz.dt.exception.HadoopAccessorException;
import com.gz.dt.service.ConfigurationService;
import com.gz.dt.service.Services;
import com.gz.dt.util.ParamChecker;
import com.gz.dt.util.XLog;
import com.gz.dt.workflow.WorkflowAction;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by naonao on 2015/10/28.
 */
public abstract class ActionExecutor {

    public static final String CONF_PREFIX = "oozie.action.";
    public static final String MAX_RETRIES = CONF_PREFIX + "retries.max";
    public static final String ACTION_RETRY_INTERVAL = CONF_PREFIX + "retry.interval";
    public static final String ACTION_RETRY_POLICY = CONF_PREFIX + "retry.policy";
    public static final String ERROR_OTHER = "OTHER";


    public static enum RETRYPOLICY {
        EXPONENTIAL, PERIODIC
    }

    private static class ErrorInfo {
        ActionExecutorException.ErrorType errorType;
        String errorCode;
        Class<?> errorClass;

        private ErrorInfo(ActionExecutorException.ErrorType errorType, String errorCode, Class<?> errorClass) {
            this.errorType = errorType;
            this.errorCode = errorCode;
            this.errorClass = errorClass;
        }
    }

    private static boolean initMode = false;
    private static Map<String, Map<String, ErrorInfo>> ERROR_INFOS = new HashMap<String, Map<String, ErrorInfo>>();

    public static final long RETRY_INTERVAL = 60;

    private String type;
    private int maxRetries;
    private long retryInterval;
    private RETRYPOLICY retryPolicy;


    protected ActionExecutor(String type) {
        this(type, RETRY_INTERVAL);
    }

    protected ActionExecutor(String type, long defaultRetryInterval) {
        this.type = ParamChecker.notEmpty(type, "type");
        this.maxRetries = ConfigurationService.getInt(MAX_RETRIES);
        int retryInterval = ConfigurationService.getInt(ACTION_RETRY_INTERVAL);
        this.retryInterval = retryInterval > 0 ? retryInterval : defaultRetryInterval;
        this.retryPolicy = getRetryPolicyFromConf();
    }

    private RETRYPOLICY getRetryPolicyFromConf() {
        String retryPolicy = ConfigurationService.get(ACTION_RETRY_POLICY);
        if (StringUtils.isBlank(retryPolicy)) {
            return RETRYPOLICY.PERIODIC;
        } else {
            try {
                return RETRYPOLICY.valueOf(retryPolicy.toUpperCase().trim());
            } catch (IllegalArgumentException e) {
                return RETRYPOLICY.PERIODIC;
            }
        }
    }

    public static void resetInitInfo() {
        if (!initMode) {
            throw new IllegalStateException("Error, action type info locked");
        }
        ERROR_INFOS.clear();
    }

    public static void enableInit() {
        initMode = true;
    }

    public static void disableInit() {
        initMode = false;
    }

    public void initActionType() {
        XLog.getLog(getClass()).trace(" Init Action Type : [{0}]", getType());
        ERROR_INFOS.put(getType(), new LinkedHashMap<String, ErrorInfo>());
    }

    public String getType() {
        return type;
    }

    public String getOozieSystemId() {
        return Services.get().getSystemId();
    }

    public String getOozieRuntimeDir() {
        return Services.get().getRuntimeDir();
    }

    public Configuration getOozieConf() {
        return Services.get().getConf();
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public RETRYPOLICY getRetryPolicy() {
        return retryPolicy;
    }

    public void setRetryPolicy(RETRYPOLICY retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public long getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
    }



    protected void registerError(String exClass, ActionExecutorException.ErrorType errorType, String errorCode) {
        if (!initMode) {
            throw new IllegalStateException("Error, action type info locked");
        }
        try {
            Class errorClass = Thread.currentThread().getContextClassLoader().loadClass(exClass);
            Map<String, ErrorInfo> executorErrorInfo = ERROR_INFOS.get(getType());
            executorErrorInfo.put(exClass, new ErrorInfo(errorType, errorCode, errorClass));
        }
        catch (ClassNotFoundException cnfe) {
            XLog.getLog(getClass()).warn(
                    "Exception [{0}] not in classpath, ActionExecutor [{1}] will handle it as ERROR", exClass,
                    getType());
        }
        catch (java.lang.NoClassDefFoundError err) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            err.printStackTrace(new PrintStream(baos));
            XLog.getLog(getClass()).warn(baos.toString());
        }
    }


    protected ActionExecutorException convertException(Exception ex) {
        if (ex instanceof ActionExecutorException) {
            return (ActionExecutorException) ex;
        }

        ActionExecutorException aee = null;
        // Check the cause of the exception first
        if (ex.getCause() != null) {
            aee = convertExceptionHelper(ex.getCause());
        }
        // If the cause isn't registered or doesn't exist, check the exception itself
        if (aee == null) {
            aee = convertExceptionHelper(ex);
            // If the cause isn't registered either, then just create a new ActionExecutorException
            if (aee == null) {
                String exClass = ex.getClass().getName();
                String errorCode = exClass.substring(exClass.lastIndexOf(".") + 1);
                aee = new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, errorCode, "{0}", ex.getMessage(), ex);
            }
        }
        return aee;
    }

    private ActionExecutorException convertExceptionHelper(Throwable ex) {
        Map<String, ErrorInfo> executorErrorInfo = ERROR_INFOS.get(getType());
        // Check if we have registered ex
        ErrorInfo classErrorInfo = executorErrorInfo.get(ex.getClass().getName());
        if (classErrorInfo != null) {
            return new ActionExecutorException(classErrorInfo.errorType, classErrorInfo.errorCode, "{0}", ex.getMessage(), ex);
        }
        // Else, check if a parent class of ex is registered
        else {
            for (ErrorInfo errorInfo : executorErrorInfo.values()) {
                if (errorInfo.errorClass.isInstance(ex)) {
                    return new ActionExecutorException(errorInfo.errorType, errorInfo.errorCode, "{0}", ex.getMessage(), ex);
                }
            }
        }
        return null;
    }


    protected String getActionSignal(WorkflowAction.Status status) {
        switch (status) {
            case OK:
                return "OK";
            case ERROR:
            case KILLED:
                return "ERROR";
            default:
                throw new IllegalArgumentException("Action status for signal can only be OK or ERROR");
        }
    }



    public abstract void start(Context context, WorkflowAction action) throws ActionExecutorException;
    //public abstract void start(Context context, String xmldef) throws ActionExecutorException;

    public abstract void end(Context context, WorkflowAction action) throws ActionExecutorException;

    public abstract void check(Context context, WorkflowAction action) throws ActionExecutorException;

    public abstract void kill(Context context, WorkflowAction action) throws ActionExecutorException;

    public abstract boolean isCompleted(String externalStatus);

    public boolean requiresNameNodeJobTracker() {
        return false;
    }

    public boolean supportsConfigurationJobXML() {
        return false;
    }



    public interface Context {

        /**
         * Create the callback URL for the action.
         *
         * @param externalStatusVar variable for the caller to inject the external status.
         * @return the callback URL.
         */
        public String getCallbackUrl(String externalStatusVar);

        /**
         * Return a proto configuration for actions with auth properties already set.
         *
         * @return a proto configuration for actions with auth properties already set.
         */
        public Configuration getProtoActionConf();

        /**
         * Return the workflow job.
         *
         * @return the workflow job.
         */
//        public WorkflowJob getWorkflow();

        /**
         * Return an ELEvaluator with the context injected.
         *
         * @return configured ELEvaluator.
         */
//        public ELEvaluator getELEvaluator();

        /**
         * Set a workflow action variable. <p> Convenience method that prefixes the variable name with the action name
         * plus a '.'.
         *
         * @param name variable name.
         * @param value variable value, <code>null</code> removes the variable.
         */
        public void setVar(String name, String value);

        /**
         * Get a workflow action variable. <p> Convenience method that prefixes the variable name with the action name
         * plus a '.'.
         *
         * @param name variable name.
         * @return the variable value, <code>null</code> if not set.
         */
        public String getVar(String name);

        /**
         * Set the action tracking information for an successfully started action.
         *
         * @param externalId the action external ID.
         * @param trackerUri the action tracker URI.
         * @param consoleUrl the action console URL.
         */
        void setStartData(String externalId, String trackerUri, String consoleUrl);


        void setExecutionData(String externalStatus, Properties actionData);


        void setExecutionStats(String jsonStats);


        void setExternalChildIDs(String externalChildIDs);


        void setEndData(WorkflowAction.Status status, String signalValue);


        boolean isRetry();


        void setExternalStatus(String externalStatus);

        /**
         * Get the Action Recovery ID.
         *
         * @return recovery ID.
         */
        String getRecoveryId();


        public Path getActionDir() throws HadoopAccessorException, IOException, URISyntaxException;


        public FileSystem getAppFileSystem() throws HadoopAccessorException, IOException, URISyntaxException;

        public void setErrorInfo(String str, String exMsg);
    }


}
