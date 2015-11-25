package com.gz.dt.servlet;

import com.gz.dt.exception.XServletException;
import com.gz.dt.util.ErrorCode;

import javax.servlet.http.HttpServletResponse;

/**
 * Created by naonao on 2015/10/27.
 */
public class ServletUtilities {

    protected static void ValidateAppPath(String wfPath, String coordPath) throws XServletException {
        if (wfPath != null && coordPath != null) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302,
                    "multiple app paths specified, only one is allowed");
        }
        else {
            if (wfPath == null && coordPath == null) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302,
                        "a workflow or coordinator app path is required");
            }
        }
    }

    protected static void ValidateAppPath(String wfPath, String coordPath, String bundlePath) throws XServletException {
        int n = 0;

        if (wfPath != null) {
            n ++;
        }

        if (coordPath != null) {
            n ++;
        }

        if (bundlePath != null) {
            n ++;
        }

        if (n == 0) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, "a workflow, coordinator, or bundle app path is required");
        }

        if (n != 1) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, "Multiple app paths specified, only one is allowed");
        }
    }

    protected static void ValidateLibPath(String libPath) throws XServletException {
        if (libPath == null) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, "a lib path is required");
        }
    }
}
