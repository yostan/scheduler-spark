package com.gz.dt.util;

import java.util.Calendar;

/**
 * Created by naonao on 2015/10/27.
 */
public enum TimeUnit {

    MINUTE(Calendar.MINUTE), HOUR(Calendar.HOUR), DAY(Calendar.DATE), MONTH(Calendar.MONTH), YEAR(Calendar.YEAR), END_OF_DAY(Calendar.DATE), END_OF_MONTH(
            Calendar.MONTH), CRON(0), NONE(-1);

    private int calendarUnit;

    private TimeUnit(int calendarUnit) {
        this.calendarUnit = calendarUnit;
    }

    public int getCalendarUnit() {
        return calendarUnit;
    }
}
