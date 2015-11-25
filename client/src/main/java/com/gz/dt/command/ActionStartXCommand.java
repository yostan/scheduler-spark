package com.gz.dt.command;

import com.gz.dt.exception.ActionExecutorException;
import com.gz.dt.exception.CommandException;
import com.gz.dt.exception.PreconditionException;
import com.gz.dt.executor.ActionExecutor;
import com.gz.dt.service.ActionService;
import com.gz.dt.service.Services;
import com.gz.dt.service.UUIDService;

/**
 * Created by naonao on 2015/10/28.
 */
public class ActionStartXCommand extends XCommand<Void> {

    private String jobId = null;
    private String actionId = null;

    private ActionExecutor executor = null;

    public ActionStartXCommand(String actionId, String type) {
        super("action.start", type, 0);
        this.actionId = actionId;
        this.jobId = Services.get().get(UUIDService.class).getId(actionId);
    }

//    public ActionStartXCommand(WorkflowJobBean job, String actionId, String type) {
//        super("action.start", type, 0);
//        this.actionId = actionId;
//        this.wfJob = job;
//        this.jobId = wfJob.getId();
//    }



    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    public String getEntityKey() {
        return this.jobId;
    }

    @Override
    protected void loadState() throws CommandException {

    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {

        executor = Services.get().get(ActionService.class).getExecutor("spark");

    }

    @Override
    protected Void execute() throws CommandException {
        try {
            executor.start(null, null);
        } catch (ActionExecutorException e) {
            e.printStackTrace();
        }
        return null;
    }
}
