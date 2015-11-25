package com.gz.dt.service;

import com.gz.dt.exception.ServiceException;
import com.gz.dt.executor.ActionExecutor;
import com.gz.dt.util.Instrumentable;
import com.gz.dt.util.Instrumentation;
import com.gz.dt.util.ParamChecker;
import com.gz.dt.util.XLog;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by naonao on 2015/10/28.
 */
public class ActionService implements Service, Instrumentable {

    public static final String CONF_ACTION_EXECUTOR_CLASSES = CONF_PREFIX + "ActionService.executor.classes";

    public static final String CONF_ACTION_EXECUTOR_EXT_CLASSES = CONF_PREFIX + "ActionService.executor.ext.classes";

    private Services services;
    private Map<String, Class<? extends ActionExecutor>> executors;
    private static XLog LOG = XLog.getLog(ActionService.class);


    public void instrument(Instrumentation instr) {
        instr.addVariable("configuration", "action.types", new Instrumentation.Variable<String>() {

            public String getValue() {
                Set<String> actionTypes = getActionTypes();
                if (actionTypes != null) {
                    return actionTypes.toString();
                }
                return "(unavailable)";
            }
        });
    }

    public void init(Services services) throws ServiceException {
        this.services = services;
        ActionExecutor.enableInit();
        ActionExecutor.resetInitInfo();
        ActionExecutor.disableInit();
        executors = new HashMap<String, Class<? extends ActionExecutor>>();

//        Class<? extends ActionExecutor>[] classes = new Class[] { StartActionExecutor.class,
//                EndActionExecutor.class, KillActionExecutor.class,  ForkActionExecutor.class, JoinActionExecutor.class };
//        registerExecutors(classes);

        Class<? extends ActionExecutor>[] classes = (Class<? extends ActionExecutor>[]) ConfigurationService.getClasses
                (services.getConf(), CONF_ACTION_EXECUTOR_CLASSES);
        registerExecutors(classes);

        classes = (Class<? extends ActionExecutor>[]) ConfigurationService.getClasses
                (services.getConf(), CONF_ACTION_EXECUTOR_EXT_CLASSES);
        registerExecutors(classes);

        initExecutors();

    }


    private void registerExecutors(Class<? extends ActionExecutor>[] classes) {
        if (classes != null) {
            for (Class<? extends ActionExecutor> executorClass : classes) {
                @SuppressWarnings("deprecation")
                ActionExecutor executor = (ActionExecutor) ReflectionUtils.newInstance(executorClass, services.getConf());
                executors.put(executor.getType(), executorClass);
            }
        }
    }


    private void initExecutors() {
        for (Class<? extends ActionExecutor> executorClass : executors.values()) {
            initExecutor(executorClass);
        }
        LOG.info("Initialized action types: " + getActionTypes());
    }


    private void initExecutor(Class<? extends ActionExecutor> klass) {
        @SuppressWarnings("deprecation")
        ActionExecutor executor = (ActionExecutor) ReflectionUtils.newInstance(klass, services.getConf());
        LOG.debug("Initializing action type [{0}] class [{1}]", executor.getType(), klass);
        ActionExecutor.enableInit();
        executor.initActionType();
        ActionExecutor.disableInit();
        LOG.trace("Initialized Executor for action type [{0}] class [{1}]", executor.getType(), klass);
    }

    Set<String> getActionTypes() {
        return executors.keySet();
    }


    public ActionExecutor getExecutor(String actionType) {
        ParamChecker.notEmpty(actionType, "actionType");
        Class<? extends ActionExecutor> executorClass = executors.get(actionType);
        return (executorClass != null) ? (ActionExecutor) ReflectionUtils.newInstance(executorClass, null) : null;
    }


    public void destroy() {
        ActionExecutor.enableInit();
        ActionExecutor.resetInitInfo();
        ActionExecutor.disableInit();
        executors = null;

    }

    public Class<? extends Service> getInterface() {
        return ActionService.class;
    }
}
