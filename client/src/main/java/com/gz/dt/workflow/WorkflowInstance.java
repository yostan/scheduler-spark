package com.gz.dt.workflow;

import com.gz.dt.exception.WorkflowException;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * Created by naonao on 2015/10/27.
 */
public interface WorkflowInstance {

    public final static String NODE_VAR_SEPARATOR = "#";

    public static enum Status {
        PREP(false),
        RUNNING(false),
        SUSPENDED(false),
        SUCCEEDED(true),
        FAILED(true),
        KILLED(true);

        private boolean isEndState;

        private Status(boolean isEndState) {
            this.isEndState = isEndState;
        }

        /**
         * Return if the status if an end state (it cannot change anymore).
         *
         * @return if the status if an end state (it cannot change anymore).
         */
        public boolean isEndState() {
            return isEndState;
        }

    }

    public Configuration getConf();

    public String getId();

    public WorkflowApp getApp();

    public boolean start() throws WorkflowException;

    public boolean signal(String path, String signaValue) throws WorkflowException;

    public void fail(String nodeName) throws WorkflowException;

    public void kill() throws WorkflowException;

    public void suspend() throws WorkflowException;

    public void resume() throws WorkflowException;

    public Status getStatus();

    public void setVar(String name, String value);

    public String getVar(String name);

    public Map<String, String> getAllVars();

    public void setAllVars(Map<String, String> varMap);

    public void setTransientVar(String name, Object value);

    public Object getTransientVar(String name);

    public String getTransition(String node);

//    public NodeDef getNodeDef(String executionPath);


}
