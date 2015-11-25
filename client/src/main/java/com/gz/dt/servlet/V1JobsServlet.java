package com.gz.dt.servlet;

import com.gz.dt.client.JsonTags;
import com.gz.dt.client.RestConstants;
import com.gz.dt.client.SubmitClient;
import com.gz.dt.engine.DagEngine;
import com.gz.dt.exception.BaseEngineException;
import com.gz.dt.exception.XServletException;
import com.gz.dt.service.DagEngineService;
import com.gz.dt.service.Services;
import com.gz.dt.util.ErrorCode;
import org.apache.hadoop.conf.Configuration;
import org.json.simple.JSONObject;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Created by naonao on 2015/10/26.
 */
public class V1JobsServlet extends BaseJobsServlet {

    private static final String INSTRUMENTATION_NAME = "v1jobs";

    public V1JobsServlet() {
        super(INSTRUMENTATION_NAME);
    }

    @Override
    JSONObject submitJob(HttpServletRequest request, Configuration conf) throws Exception {
        JSONObject json = new JSONObject();

        String jobType = request.getParameter(RestConstants.JOBTYPE_PARAM);

        if (jobType == null) {
            String wfPath = conf.get(SubmitClient.APP_PATH);
            String coordPath = conf.get(SubmitClient.COORDINATOR_APP_PATH);
            String bundlePath = conf.get(SubmitClient.BUNDLE_APP_PATH);

            ServletUtilities.ValidateAppPath(wfPath, coordPath, bundlePath);

            if (wfPath != null) {
                json = submitWorkflowJob(request, conf);
            }
            else if (coordPath != null) {
//                json = submitCoordinatorJob(request, conf);
                json.put("CoordinatorJob", "ok");
            }
            else {
//                json = submitBundleJob(request, conf);
                json.put("BundleJob", "ok");
            }
        }


        //json.put("Requsrt", "ok");
        return json;
    }





    private JSONObject submitWorkflowJob(HttpServletRequest request, Configuration conf) throws XServletException {

        JSONObject json = new JSONObject();

        try {
            String action = request.getParameter(RestConstants.ACTION_PARAM);
            if (action != null && !action.equals(RestConstants.JOB_ACTION_START)
                    && !action.equals(RestConstants.JOB_ACTION_DRYRUN)) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                        RestConstants.ACTION_PARAM, action);
            }
            boolean startJob = (action != null);
            String user = conf.get(SubmitClient.USER_NAME);
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(user);
            String id;
            boolean dryrun = false;
            if (action != null) {
                dryrun = (action.equals(RestConstants.JOB_ACTION_DRYRUN));
            }
            if (dryrun) {
                id = dagEngine.dryRunSubmit(conf);
            }
            else {
                id = dagEngine.submitJob(conf, startJob);
            }
            json.put(JsonTags.JOB_ID, id);
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }


}
