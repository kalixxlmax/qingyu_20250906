package com.basic.im.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class LocalDateUtil {

    public final static String YEAR_MONTH_PATTERN="yyyyMM";
    public final static String YEAR_MONTH_DAY_PATTERN="yyyy-MM-dd";

    public static String formatDate(LocalDate date) {
        return formatDate(date, YEAR_MONTH_PATTERN);
    }

    public static String formatDate(LocalDate date, String pattern) {
        return date.format(DateTimeFormatter.ofPattern(pattern));
    }

    public static LocalDate dateToLocalDate(long timeSeconds) {
        Date  date = new Date(timeSeconds*1000);
        Instant instant = date.toInstant();
        ZoneId zoneId = ZoneId.systemDefault();
        return instant.atZone(zoneId).toLocalDate();
    }

    public static long localDateToDate(LocalDateTime localDateTime) {
        ZoneId zone = ZoneId.systemDefault();
        Instant instant = localDateTime.atZone(zone).toInstant();
        return Date.from(instant).getTime()/1000;
    }

}
