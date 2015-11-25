package com.gz.dt.service;

import com.gz.dt.exception.ServiceException;
import com.gz.dt.util.ErrorCode;
import com.gz.dt.util.ParamChecker;
import com.gz.dt.util.XLog;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by naonao on 2015/10/28.
 */
public class UUIDService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "UUIDService.";
    public static final String CONF_GENERATOR = CONF_PREFIX + "generator";

    protected String startTime;
    private AtomicLong counter;
    private String systemId;


    public void init(Services services) throws ServiceException {
        String genType = ConfigurationService.get(services.getConf(), CONF_GENERATOR).trim();
        if (genType.equals("counter")) {
            counter = new AtomicLong();
            resetStartTime();
        }
        else {
            if (!genType.equals("random")) {
                throw new ServiceException(ErrorCode.E0120, genType);
            }
        }
        systemId = services.getSystemId();

    }

    protected void resetStartTime() {
        startTime = new SimpleDateFormat("yyMMddHHmmssSSS").format(new Date());
    }

    public void destroy() {
        counter = null;
        startTime = null;

    }

    public Class<? extends Service> getInterface() {
        return UUIDService.class;
    }


    public String getId(String childId) {
        int index = ParamChecker.notEmpty(childId, "childId").indexOf("@");
        if (index == -1) {
            throw new IllegalArgumentException(XLog.format("invalid child id [{0}]", childId));
        }
        return childId.substring(0, index);
    }

}
