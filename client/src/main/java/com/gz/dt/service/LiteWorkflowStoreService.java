package com.gz.dt.service;

/**
 * Created by naonao on 2015/10/28.
 */
public abstract class LiteWorkflowStoreService extends WorkflowStoreService {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "LiteWorkflowStoreService.";
    public static final String CONF_PREFIX_USER_RETRY = CONF_PREFIX + "user.retry.";
    public static final String CONF_USER_RETRY_MAX = CONF_PREFIX_USER_RETRY + "max";
    public static final String CONF_USER_RETRY_INTEVAL = CONF_PREFIX_USER_RETRY + "inteval";
    public static final String CONF_USER_RETRY_ERROR_CODE = CONF_PREFIX_USER_RETRY + "error.code";
    public static final String CONF_USER_RETRY_ERROR_CODE_EXT = CONF_PREFIX_USER_RETRY + "error.code.ext";

    public static final String NODE_DEF_VERSION_0 = "_oozie_inst_v_0";
    public static final String NODE_DEF_VERSION_1 = "_oozie_inst_v_1";
    public static final String CONF_NODE_DEF_VERSION = CONF_PREFIX + "node.def.version";

    public static final String USER_ERROR_CODE_ALL = "ALL";


}
