package com.gz.dt.util;

import com.gz.dt.service.ConfigurationService;
import org.apache.hadoop.conf.Configuration;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by naonao on 2015/10/27.
 */
public class DateUtils {

    private static final Pattern GMT_OFFSET_COLON_PATTERN = Pattern.compile("^GMT(\\-|\\+)(\\d{2})(\\d{2})$");

    public static final TimeZone UTC = getTimeZone("UTC");

    public static final String ISO8601_UTC_MASK = "yyyy-MM-dd'T'HH:mm'Z'";
    private static final String ISO8601_TZ_MASK_WITHOUT_OFFSET = "yyyy-MM-dd'T'HH:mm";

    private static String ACTIVE_MASK = ISO8601_UTC_MASK;
    private static TimeZone ACTIVE_TIMEZONE = UTC;

    public static final String OOZIE_PROCESSING_TIMEZONE_KEY = "oozie.processing.timezone";

    public static final String OOZIE_PROCESSING_TIMEZONE_DEFAULT = "UTC";

    private static boolean OOZIE_IN_UTC = true;

    private static final Pattern VALID_TIMEZONE_PATTERN = Pattern.compile("^UTC$|^GMT(\\+|\\-)\\d{4}$");


    public static void setConf(Configuration conf) {
        String tz = ConfigurationService.get(conf, OOZIE_PROCESSING_TIMEZONE_KEY);
        if (!VALID_TIMEZONE_PATTERN.matcher(tz).matches()) {
            throw new RuntimeException("Invalid Oozie timezone, it must be 'UTC' or 'GMT(+/-)####");
        }
        ACTIVE_TIMEZONE = TimeZone.getTimeZone(tz);
        OOZIE_IN_UTC = ACTIVE_TIMEZONE.equals(UTC);
        ACTIVE_MASK = (OOZIE_IN_UTC) ? ISO8601_UTC_MASK : ISO8601_TZ_MASK_WITHOUT_OFFSET + tz.substring(3);
    }


    public static TimeZone getOozieProcessingTimeZone() {
        return ACTIVE_TIMEZONE;
    }


    public static String getOozieTimeMask() {
        return ACTIVE_MASK;
    }

    private static DateFormat getISO8601DateFormat(TimeZone tz, String mask) {
        DateFormat dateFormat = new SimpleDateFormat(mask);
        // Stricter parsing to prevent dates such as 2011-12-50T01:00Z (December 50th) from matching
        dateFormat.setLenient(false);
        dateFormat.setTimeZone(tz);
        return dateFormat;
    }

    private static DateFormat getSpecificDateFormat(String format) {
        DateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(ACTIVE_TIMEZONE);
        return dateFormat;
    }


    private static String handleGMTOffsetTZNames(String tzId) {
        Matcher m = GMT_OFFSET_COLON_PATTERN.matcher(tzId);
        if (m.matches() && m.groupCount() == 3) {
            tzId = "GMT" + m.group(1) + m.group(2) + ":" + m.group(3);
        }
        return tzId;
    }


    public static TimeZone getTimeZone(String tzId) {
        if (tzId == null) {
            throw new IllegalArgumentException("Invalid TimeZone: " + tzId);
        }
        tzId = handleGMTOffsetTZNames(tzId);    // account for GMT-####
        TimeZone tz = TimeZone.getTimeZone(tzId);
        // If these are not equal, it means that the tzId is not valid (invalid tzId's return GMT)
        if (!tz.getID().equals(tzId)) {
            throw new IllegalArgumentException("Invalid TimeZone: " + tzId);
        }
        return tz;
    }


    public static Date parseDateUTC(String s) throws ParseException {
        return getISO8601DateFormat(UTC, ISO8601_UTC_MASK).parse(s);
    }


    public static Date parseDateOozieTZ(String s) throws ParseException {
        s = s.trim();
        ParsePosition pos = new ParsePosition(0);
        Date d = getISO8601DateFormat(ACTIVE_TIMEZONE, ACTIVE_MASK).parse(s, pos);
        if (d == null) {
            throw new ParseException("Could not parse [" + s + "] using [" + ACTIVE_MASK + "] mask",
                    pos.getErrorIndex());
        }
        if (d != null && s.length() > pos.getIndex()) {
            throw new ParseException("Correct datetime string is followed by invalid characters: " + s, pos.getIndex());
        }
        return d;
    }


    public static String formatDateOozieTZ(Date d) {
        return (d != null) ? getISO8601DateFormat(ACTIVE_TIMEZONE, ACTIVE_MASK).format(d) : "NULL";
    }


    public static String formatDateCustom(Date d, String format) {
        return (d != null) ? getSpecificDateFormat(format).format(d) : "NULL";
    }

    public static String formatDateOozieTZ(Calendar c) {
        return (c != null) ? formatDateOozieTZ(c.getTime()) : "NULL";
    }


    public static String formatDate(Calendar c) {
        return (c != null) ? getISO8601DateFormat(c.getTimeZone(), ACTIVE_MASK).format(c.getTime()) : "NULL";
    }


    public static int hoursInDay(Calendar cal) {
        Calendar localCal = new GregorianCalendar(cal.getTimeZone());
        localCal.set(Calendar.MILLISECOND, 0);
        localCal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), 0, 30, 0);
        localCal.add(Calendar.HOUR_OF_DAY, 24);
        switch (localCal.get(Calendar.HOUR_OF_DAY)) {
            case 1:
                return 23;
            case 23:
                return 25;
            default: // Case 0
                return 24;
        }
    }


    public static boolean isDSTChangeDay(Calendar cal) {
        return hoursInDay(cal) != 24;
    }


    public static void moveToEnd(Calendar cal, TimeUnit endOfFlag) {
        // TODO: Both logic needs to be checked
        if (endOfFlag == TimeUnit.END_OF_DAY) { // 24:00:00
            cal.add(Calendar.DAY_OF_MONTH, 1);
            // cal.set(Calendar.HOUR_OF_DAY, cal
            // .getActualMaximum(Calendar.HOUR_OF_DAY) + 1);// TODO:
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
        }
        else {
            if (endOfFlag == TimeUnit.END_OF_MONTH) {
                cal.add(Calendar.MONTH, 1);
                cal.set(Calendar.DAY_OF_MONTH, 1);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
            }
        }
    }


    public static Calendar getCalendar(String dateString, TimeZone tz) throws Exception {
        Date date = DateUtils.parseDateOozieTZ(dateString);
        Calendar calDate = Calendar.getInstance();
        calDate.setTime(date);
        calDate.setTimeZone(tz);
        return calDate;
    }


    public static Calendar getCalendar(String dateString) throws Exception {
        return getCalendar(dateString, ACTIVE_TIMEZONE);
    }


    public static java.util.Date toDate(java.sql.Timestamp timestamp) {
        if (timestamp != null) {
            long milliseconds = timestamp.getTime();
            return new java.util.Date(milliseconds);
        }
        return null;
    }


    public static Timestamp convertDateToTimestamp(Date d) {
        if (d != null) {
            return new Timestamp(d.getTime());
        }
        return null;
    }

}
