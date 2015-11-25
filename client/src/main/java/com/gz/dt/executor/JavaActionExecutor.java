package com.gz.dt.executor;

import com.gz.dt.exception.ActionExecutorException;
import com.gz.dt.workflow.WorkflowAction;

/**
 * Created by naonao on 2015/10/28.
 */
public class JavaActionExecutor extends ActionExecutor {

    public JavaActionExecutor() {
        this("java");
    }

    protected JavaActionExecutor(String type) {
        super(type);
    }



    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {

        submitLauncher(context, action);

    }

    public void submitLauncher(Context context, WorkflowAction action) throws ActionExecutorException {

    }








    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {

    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {

    }

    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {

    }

    @Override
    public boolean isCompleted(String externalStatus) {
        return false;
    }
}
