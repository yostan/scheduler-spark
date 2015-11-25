package com.gz.dt.workflow;

import com.gz.dt.exception.WorkflowException;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by naonao on 2015/10/27.
 */
public interface WorkflowLib {

    public WorkflowApp parseDef(String wfXml, Configuration jobConf, Configuration configDefault)
            throws WorkflowException;

    public WorkflowInstance createInstance(WorkflowApp app, Configuration conf) throws WorkflowException;

    public WorkflowInstance createInstance(WorkflowApp app, Configuration conf, String wfId)
            throws WorkflowException;

    public void insert(WorkflowInstance instance) throws WorkflowException;

    public WorkflowInstance get(String id) throws WorkflowException;

    public void update(WorkflowInstance instance) throws WorkflowException;

    public void delete(String id) throws WorkflowException;

    public void commit() throws WorkflowException;

    public void close() throws WorkflowException;


}
