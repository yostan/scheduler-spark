package com.gz.dt.workflow;

import com.gz.dt.exception.WorkflowException;
import org.apache.hadoop.conf.Configuration;

import javax.xml.validation.Schema;
import java.sql.Connection;

/**
 * Created by naonao on 2015/10/28.
 */
public class DBLiteWorkflowLib extends LiteWorkflowLib {

    private final Connection connection;

    public DBLiteWorkflowLib(Schema schema/*,
                             Class<? extends ControlNodeHandler> controlNodeHandler,
                             Class<? extends DecisionNodeHandler> decisionHandlerClass,
                             Class<? extends ActionNodeHandler> actionHandlerClass*/, Connection connection) {
        super(schema/*, controlNodeHandler, decisionHandlerClass, actionHandlerClass*/);
        this.connection = connection;
    }



    public WorkflowInstance createInstance(WorkflowApp app, Configuration conf) throws WorkflowException {
        return null;
    }

    public WorkflowInstance createInstance(WorkflowApp app, Configuration conf, String wfId) throws WorkflowException {
        return null;
    }

    public void insert(WorkflowInstance instance) throws WorkflowException {

    }

    public WorkflowInstance get(String id) throws WorkflowException {
        return null;
    }

    public void update(WorkflowInstance instance) throws WorkflowException {

    }

    public void delete(String id) throws WorkflowException {

    }

    public void commit() throws WorkflowException {

    }

    public void close() throws WorkflowException {

    }
}
