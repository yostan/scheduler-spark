package com.gz.dt.util;

/**
 * Created by naonao on 2015/10/22.
 */
public class ParamChecker {

    public static <T> T notNull(T obj, String name) {
        if (obj == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
        return obj;
    }

    public static String notEmpty(String str, String name) {
        return notEmpty(str, name, null);
    }


    public static String notEmpty(String str, String name, String info) {
        if (str == null) {
            throw new IllegalArgumentException(name + " cannot be null" + (info == null ? "" : ", " + info));
        }
        if (str.length() == 0) {
            throw new IllegalArgumentException(name + " cannot be empty" + (info == null ? "" : ", " + info));
        }
        return str;
    }


}
