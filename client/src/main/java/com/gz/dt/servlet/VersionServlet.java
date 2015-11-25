package com.gz.dt.servlet;

import com.gz.dt.client.SubmitClient;
import org.json.simple.JSONArray;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by naonao on 2015/10/22.
 */
public class VersionServlet extends JsonRestServlet {

    private static final String INSTRUMENTATION_NAME = "version";

    private static final ResourceInfo RESOURCE_INFO =
            new ResourceInfo("", Arrays.asList("GET"), Collections.EMPTY_LIST);


    public VersionServlet() {
        super(INSTRUMENTATION_NAME, RESOURCE_INFO);
        // versions.add(OozieClient.WS_PROTOCOL_VERSION_0);
        // versions.add(OozieClient.WS_PROTOCOL_VERSION);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        JSONArray versions = new JSONArray();
        versions.add(SubmitClient.WS_PROTOCOL_VERSION_0);
        versions.add(SubmitClient.WS_PROTOCOL_VERSION_1);
        versions.add(SubmitClient.WS_PROTOCOL_VERSION);
        sendJsonResponse(response, HttpServletResponse.SC_OK, versions);
    }
}
