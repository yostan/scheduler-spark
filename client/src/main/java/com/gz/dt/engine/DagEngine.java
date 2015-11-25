package com.gz.dt.engine;

import com.gz.dt.client.SubmitClient;
import com.gz.dt.command.StartXCommand;
import com.gz.dt.command.SubmitXCommand;
import com.gz.dt.exception.BaseEngineException;
import com.gz.dt.exception.CommandException;
import com.gz.dt.exception.DagEngineException;
import com.gz.dt.util.ErrorCode;
import com.gz.dt.util.ParamChecker;
import com.gz.dt.util.XLog;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by naonao on 2015/10/27.
 */
public class DagEngine {

    private static final int HIGH_PRIORITY = 2;
    private static XLog LOG = XLog.getLog(DagEngine.class);

    private String user;

    public DagEngine() {
    }

    public DagEngine(String user) {
        this();
        this.user = ParamChecker.notEmpty(user, "user");
    }

    public String submitJob(Configuration conf, boolean startJob) throws DagEngineException {
        validateSubmitConfiguration(conf);

        try {
            String jobId;
            SubmitXCommand submit = new SubmitXCommand(conf);
            jobId = submit.call();
//            if (startJob) {
//                start(jobId);
//            }
            return jobId;
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }


    public void start(String jobId) throws DagEngineException {
        // Changing to synchronous call from asynchronous queuing to prevent the
        // loss of command if the queue is full or the queue is lost in case of
        // failure.
        try {
            new StartXCommand(jobId).call();
        }
        catch (CommandException e) {
            throw new DagEngineException(e);
        }
    }



    public String dryRunSubmit(Configuration conf) throws BaseEngineException {
        try {
            SubmitXCommand submit = new SubmitXCommand(true, conf);
            return submit.call();
        } catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }


    private void validateSubmitConfiguration(Configuration conf) throws DagEngineException {
        if (conf.get(SubmitClient.APP_PATH) == null) {
            throw new DagEngineException(ErrorCode.E0401, SubmitClient.APP_PATH);
        }
    }
}
