package com.gz.dt.service;

import com.gz.dt.exception.ServiceException;
import com.gz.dt.util.Instrumentable;
import com.gz.dt.util.Instrumentation;
import com.gz.dt.util.XLog;
import com.gz.dt.workflow.DBLiteWorkflowLib;
import com.gz.dt.workflow.WorkflowLib;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by naonao on 2015/10/28.
 */
public class DBLiteWorkflowStoreService extends LiteWorkflowStoreService implements Instrumentable {

    private boolean selectForUpdate;
    private XLog log;
    private int statusWindow;

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "DBLiteWorkflowStoreService.";
    public static final String CONF_METRICS_INTERVAL_MINS = CONF_PREFIX + "status.metrics.collection.interval";
    public static final String CONF_METRICS_INTERVAL_WINDOW = CONF_PREFIX + "status.metrics.window";

    private static final String INSTRUMENTATION_GROUP = "jobstatus";
    private static final String INSTRUMENTATION_GROUP_WINDOW = "windowjobstatus";

    private Map<String, Integer> statusCounts = new HashMap<String, Integer>();
    private Map<String, Integer> statusWindowCounts = new HashMap<String, Integer>();



    public void instrument(Instrumentation instr) {

    }

    @Override
    public WorkflowLib getWorkflowLibWithNoDB() {
        return getWorkflowLib(null);
    }

    private WorkflowLib getWorkflowLib(Connection conn) {
        javax.xml.validation.Schema schema = Services.get().get(SchemaService.class).getSchema(SchemaService.SchemaName.WORKFLOW);
        return new DBLiteWorkflowLib(schema/*, LiteControlNodeHandler.class,
                LiteDecisionHandler.class, LiteActionHandler.class*/, conn);
    }



    public void init(Services services) throws ServiceException {

    }

    public void destroy() {

    }
}
