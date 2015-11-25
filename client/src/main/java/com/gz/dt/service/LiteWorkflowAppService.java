package com.gz.dt.service;

import com.gz.dt.client.SubmitClient;
import com.gz.dt.exception.WorkflowException;
import com.gz.dt.util.ParamChecker;
import com.gz.dt.workflow.WorkflowApp;
import com.gz.dt.workflow.WorkflowLib;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by naonao on 2015/10/27.
 */
public class LiteWorkflowAppService extends WorkflowAppService {


    @Override
    public WorkflowApp parseDef(Configuration jobConf) throws WorkflowException {
        return parseDef(jobConf, null);
    }

    @Override
    public WorkflowApp parseDef(Configuration jobConf, Configuration configDefault) throws WorkflowException {
        String appPath = ParamChecker.notEmpty(jobConf.get(SubmitClient.APP_PATH), SubmitClient.APP_PATH);
        String user = ParamChecker.notEmpty(jobConf.get(SubmitClient.USER_NAME), SubmitClient.USER_NAME);
        String workflowXml = readDefinition(appPath, user, jobConf);
        return parseDef(workflowXml, jobConf, configDefault);
    }

    @Override
    public WorkflowApp parseDef(String wfXml, Configuration jobConf) throws WorkflowException {
        return parseDef(wfXml, jobConf, null);
    }

    public WorkflowApp parseDef(String workflowXml, Configuration jobConf, Configuration configDefault)
            throws WorkflowException {
        WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();
        return workflowLib.parseDef(workflowXml, jobConf, configDefault);
    }

}
