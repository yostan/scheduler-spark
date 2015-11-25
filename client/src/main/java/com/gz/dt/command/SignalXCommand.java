package com.gz.dt.command;

import com.gz.dt.exception.CommandException;
import com.gz.dt.exception.PreconditionException;
import com.gz.dt.util.ParamChecker;

/**
 * Created by naonao on 2015/10/28.
 */
public class SignalXCommand extends XCommand<Void> {

    private String jobId;
    private String actionId;

    public SignalXCommand(String name, int priority, String jobId) {
        super(name, name, priority);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
    }

    public SignalXCommand(String jobId, String actionId) {
        this("signal", 1, jobId);
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
    }






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

    }

    @Override
    protected Void execute() throws CommandException {
//        new ActionStartXCommand(wfJob, syncAction.getId(), syncAction.getType()).call(getEntityKey());
        return null;
    }
}
