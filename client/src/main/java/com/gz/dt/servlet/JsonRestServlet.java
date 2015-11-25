package com.gz.dt.servlet;

import com.gz.dt.client.RestConstants;
import com.gz.dt.util.ParamChecker;
import org.json.simple.JSONStreamAware;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

/**
 * Created by naonao on 2015/10/22.
 */
public abstract class JsonRestServlet extends HttpServlet {
    static final String JSON_UTF8 = RestConstants.JSON_CONTENT_TYPE + "; charset=\"UTF-8\"";
    protected static final String XML_UTF8 = RestConstants.XML_CONTENT_TYPE + "; charset=\"UTF-8\"";
    protected static final String TEXT_UTF8 = RestConstants.TEXT_CONTENT_TYPE + "; charset=\"UTF-8\"";

    protected static final String AUDIT_OPERATION = "audit.operation";
    protected static final String AUDIT_PARAM = "audit.param";
    protected static final String AUDIT_ERROR_CODE = "audit.error.code";
    protected static final String AUDIT_ERROR_MESSAGE = "audit.error.message";
    protected static final String AUDIT_HTTP_STATUS_CODE = "audit.http.status.code";

    private String instrumentationName;
    private List<ResourceInfo> resourcesInfo = new ArrayList<ResourceInfo>();


    public JsonRestServlet(String instrumentationName, ResourceInfo... resourcesInfo) {
        this.instrumentationName = ParamChecker.notEmpty(instrumentationName, "instrumentationName");
        if (resourcesInfo.length == 0) {
            throw new IllegalArgumentException("There must be at least one ResourceInfo");
        }
        this.resourcesInfo = Arrays.asList(resourcesInfo);
    }


    protected void sendJsonResponse(HttpServletResponse response, int statusCode, JSONStreamAware json)
            throws IOException {
        if (statusCode == HttpServletResponse.SC_OK || statusCode == HttpServletResponse.SC_CREATED) {
            response.setStatus(statusCode);
        }
        else {
            response.sendError(statusCode);
        }
        response.setStatus(statusCode);
        response.setContentType(JSON_UTF8);
        json.writeJSONString(response.getWriter());
    }


    protected String validateContentType(HttpServletRequest request, String expected) throws ServletException {
        String contentType = null;
        String contentTypes = request.getContentType();
        if(contentTypes == null || contentTypes.trim().length() == 0) {
            throw new ServletException();
        }
        int index = contentTypes.indexOf(";");
        if(index > -1) {
            contentType = contentTypes.substring(0, index);
        }
        contentType = contentType.toLowerCase();

        if(!contentType.equals(expected)) {
            throw new ServletException();
        }
        return contentType;
    }







    public static class ParameterInfo {
        private String name;
        private Class type;
        private List<String> methods;
        private boolean required;

        public ParameterInfo(String name, Class type, boolean required, List<String> methods) {
            this.name = ParamChecker.notEmpty(name, "name");
            if (type != Integer.class && type != Boolean.class && type != String.class) {
                throw new IllegalArgumentException("Type must be integer, boolean or string");
            }
            this.type = ParamChecker.notNull(type, "type");
            this.required = required;
            this.methods = ParamChecker.notNull(methods, "methods");
        }
    }


    /**
     * This bean defines a REST resource.
     */
    public static class ResourceInfo {
        private String name;
        private boolean wildcard;
        private List<String> methods;
        private Map<String, ParameterInfo> parameters = new HashMap<String, ParameterInfo>();

        public ResourceInfo(String name, List<String> methods, List<ParameterInfo> parameters) {
            this.name = name;
            wildcard = name.equals("*");
            for (ParameterInfo parameter : parameters) {
                this.parameters.put(parameter.name, parameter);
            }
            this.methods = ParamChecker.notNull(methods, "methods");
        }
    }




}
