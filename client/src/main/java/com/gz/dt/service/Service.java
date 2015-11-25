package com.gz.dt.service;

import com.gz.dt.exception.ServiceException;

/**
 * Created by naonao on 2015/10/26.
 */
public interface Service {

    public static final String DEFAULT_LOCK_TIMEOUT = "oozie.service.default.lock.timeout";

    public static final String CONF_PREFIX = "oozie.service.";

    public void init(Services services) throws ServiceException;

    public void destroy();

    public Class<? extends Service> getInterface();

    public static long lockTimeout = ConfigurationService.getLong(DEFAULT_LOCK_TIMEOUT);
}
