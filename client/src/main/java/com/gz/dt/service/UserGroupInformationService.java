package com.gz.dt.service;

import com.gz.dt.exception.ServiceException;
import com.gz.dt.util.XLog;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by naonao on 2015/10/27.
 */
public class UserGroupInformationService implements Service {

    private ConcurrentMap<String, UserGroupInformation> cache = new ConcurrentHashMap<String, UserGroupInformation>();


    public UserGroupInformation getProxyUser(String user) throws IOException {
        cache.putIfAbsent(user, UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser()));
        return cache.get(user);
    }

    public void init(Services services) throws ServiceException {

    }

    public void destroy() {
        for (UserGroupInformation ugi : cache.values()) {
            try {
                FileSystem.closeAllForUGI(ugi);
            } catch(IOException ioe) {
                XLog.getLog(this.getClass()).warn("Exception occurred while closing filesystems for " + ugi.getUserName(), ioe);
            }
        }
        cache.clear();

    }

    public Class<? extends Service> getInterface() {
        return UserGroupInformationService.class;
    }

}
