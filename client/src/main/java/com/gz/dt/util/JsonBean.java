package com.gz.dt.util;

import org.json.simple.JSONObject;

/**
 * Created by naonao on 2015/10/27.
 */
public interface JsonBean {

    public JSONObject toJSONObject();

    public JSONObject toJSONObject(String timeZoneId);
}
