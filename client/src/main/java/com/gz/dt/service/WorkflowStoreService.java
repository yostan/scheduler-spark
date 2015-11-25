package com.gz.dt.service;

import com.gz.dt.exception.StoreException;
import com.gz.dt.workflow.WorkflowInstance;
import com.gz.dt.workflow.WorkflowLib;

import java.util.Collections;
import java.util.List;

/**
 * Created by naonao on 2015/10/27.
 */
public abstract class WorkflowStoreService implements Service {

    public final static String TRANSIENT_VAR_PREFIX = "oozie.workflow.";
    public static final String WORKFLOW_BEAN = TRANSIENT_VAR_PREFIX + "workflow.bean";
    final static String ACTION_ID = "action.id";
    final static String ACTIONS_TO_KILL = TRANSIENT_VAR_PREFIX + "actions.to.kill";
    final static String ACTIONS_TO_FAIL = TRANSIENT_VAR_PREFIX + "actions.to.fail";
    final static String ACTIONS_TO_START = TRANSIENT_VAR_PREFIX + "actions.to.start";


    public Class<? extends Service> getInterface() {
        return WorkflowStoreService.class;
    }


//    public <S extends Store> WorkflowStore create(S store) throws StoreException {
//        return null;
//    }
//
//
//    public static List<WorkflowActionBean> getActionsToStart(WorkflowInstance instance) {
//        List<WorkflowActionBean> list = (List<WorkflowActionBean>) instance.getTransientVar(ACTIONS_TO_START);
//        instance.setTransientVar(ACTIONS_TO_START, null);
//        return (list != null) ? list : Collections.EMPTY_LIST;
//    }


    public static List<String> getActionsToKill(WorkflowInstance instance) {
        List<String> list = (List<String>) instance.getTransientVar(ACTIONS_TO_KILL);
        instance.setTransientVar(ACTIONS_TO_KILL, null);
        return (list != null) ? list : Collections.EMPTY_LIST;
    }


    public static List<String> getActionsToFail(WorkflowInstance instance) {
        List<String> list = (List<String>) instance.getTransientVar(ACTIONS_TO_FAIL);
        instance.setTransientVar(ACTIONS_TO_FAIL, null);
        return (list != null) ? list : Collections.EMPTY_LIST;
    }



    public abstract WorkflowLib getWorkflowLibWithNoDB();

//    public abstract WorkflowStore create() throws StoreException;


}
