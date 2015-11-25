package com.gz.dt.servlet;

import com.gz.dt.client.RestConstants;
import com.gz.dt.util.XConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.json.simple.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by naonao on 2015/10/26.
 */
public abstract class BaseJobsServlet extends JsonRestServlet {
    private static final JsonRestServlet.ResourceInfo RESOURCES_INFO[] = new JsonRestServlet.ResourceInfo[1];

    static {
        RESOURCES_INFO[0] = new JsonRestServlet.ResourceInfo("", Arrays.asList(
                "POST", "GET", "PUT"), Arrays.asList(
                new JsonRestServlet.ParameterInfo(RestConstants.ACTION_PARAM,
                        String.class, false, Arrays.asList("POST", "PUT")),
                new JsonRestServlet.ParameterInfo(
                        RestConstants.JOBS_FILTER_PARAM, String.class, false,
                        Arrays.asList("GET", "PUT")),
                new JsonRestServlet.ParameterInfo(RestConstants.JOBTYPE_PARAM,
                        String.class, false, Arrays.asList("GET", "POST", "PUT")),
                new JsonRestServlet.ParameterInfo(RestConstants.OFFSET_PARAM,
                        String.class, false, Arrays.asList("GET", "PUT")),
                new JsonRestServlet.ParameterInfo(RestConstants.LEN_PARAM,
                        String.class, false, Arrays.asList("GET", "PUT")),
                new JsonRestServlet.ParameterInfo(RestConstants.JOBS_BULK_PARAM,
                        String.class, false, Arrays.asList("GET", "PUT")),
                new JsonRestServlet.ParameterInfo(
                        RestConstants.JOBS_EXTERNAL_ID_PARAM, String.class,
                        false, Arrays.asList("GET"))));
    }

    public BaseJobsServlet(String instrumentationName) {
        super(instrumentationName, RESOURCES_INFO);
    }


    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        //validateContentType(request, RestConstants.XML_CONTENT_TYPE);

        request.setAttribute(AUDIT_OPERATION, request
                .getParameter(RestConstants.ACTION_PARAM));

        XConfiguration conf = new XConfiguration(request.getInputStream());
        conf.set("user.name", "bigdata");

        JSONObject json = null;
        try {
            json = submitJob(request, conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

        sendJsonResponse(response, HttpServletResponse.SC_CREATED, json);
    }



    abstract JSONObject submitJob(HttpServletRequest request, Configuration conf) throws Exception;
}
