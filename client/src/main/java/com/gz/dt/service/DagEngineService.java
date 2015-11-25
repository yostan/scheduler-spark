package com.gz.dt.service;

import com.gz.dt.engine.DagEngine;
import com.gz.dt.exception.ServiceException;

/**
 * Created by naonao on 2015/10/27.
 */
public class DagEngineService implements Service {
    public void init(Services services) throws ServiceException {

    }

    public void destroy() {

    }

    public Class<? extends Service> getInterface() {
        return DagEngineService.class;
    }

    public DagEngine getDagEngine(String user) {
        return new DagEngine(user);
    }

    public DagEngine getSystemDagEngine() {
        return new DagEngine();
    }
}
