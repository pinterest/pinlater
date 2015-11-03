/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.pinlater.commons.util;

import com.twitter.common.util.Clock;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;


/**
 * General utilities for date/time conversions.
 */
public class TimeUtils {

  public static final int HOURS_OF_ONE_DAY = 24;
  public static final int MINUTES_OF_ONE_HOUR = 60;
  public static final int MINUTES_OF_ONE_DAY = 60 * 24;
  public static final int SECONDS_OF_ONE_HOUR = 3600;
  public static final int SECONDS_OF_ONE_DAY = 24 * 3600;
  public static final int SECONDS_OF_ONE_WEEK = 7 * 24 * 3600;
  public static final int SECONDS_OF_TWO_WEEKS = 14 * 24 * 3600;
  public static final int SECONDS_OF_FOUR_WEEKS = 28 * 24 * 3600;
  public static final int SECONDS_OF_NINETY_DAYS = 90 * 24 * 3600;

  private static DateFormat sFormatterInSec = createUTCDateFormatter("yyyy-MM-dd HH:mm:ss");
  private static DateFormat sFormatterInMin = createUTCDateFormatter("yyyy-MM-dd HH:mm");
  private static DateFormat sFormatterInDay = createUTCDateFormatter("yyyy-MM-dd");

  /**
   * Creates a DateFormat that assumes the incoming date is in UTC time.
   * @param dateFormat - the format string, e.g. "yyyy-MM-dd".
   */
  public static DateFormat createUTCDateFormatter(String dateFormat) {
    DateFormat formatter = new SimpleDateFormat(dateFormat);
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    return formatter;
  }

  /**
   * Get the current timestamp in milliseconds.
   */
  public static long getNowTimestampInMilliseconds() {
    Date date = new Date();
    return date.getTime();
  }

  /**
   * More accurate form of current time in milliseconds, based on System.nanoTime().
   * NOTE: This is not wall clock time, and should only be used for relative time measurement.
   */
  public static long millisTime() {
    return TimeUnit.NANOSECONDS.toMillis(Clock.SYSTEM_CLOCK.nowNanos());
  }

  /*
   * Example input: 2013-06-08 23:59:59 and 2013-06-09 00:00:01
   * Example output: 1
   */
  public static long getDiffInDays(Date date1, Date date2) {
    long ts1 = stringInDaysToTimestampInSeconds(dateToStringInDays(date1));
    long ts2 = stringInDaysToTimestampInSeconds(dateToStringInDays(date2));
    return Math.abs(ts1 - ts2) / SECONDS_OF_ONE_DAY;
  }

  /*
   * Example input: timestamp representing 2013-06-08 23:59:59 and 2013-06-09 00:00:01
   * Example output: 1
   */
  public static long getDiffInDays(long ts1, long ts2) {
    return getDiffInDays(timestampInSecondsToDate(ts1), timestampInSecondsToDate(ts2));
  }

  /**
   * Get the current timestamp in seconds.
   */
  public static long getNowTimestampInSeconds() {
    return getNowTimestampInMilliseconds() / 1000;
  }

  /**
   * Convert a date time string (e.g., "2011-06-07 19:12:18") to a Date object.
   */
  public static Date stringToDate(String s, DateFormat formatter) {
    try {
      return formatter.parse(s);
    } catch (ParseException e) {
      return null;
    }
  }

  public static long getStartTimestampOfADay(long timestamp) {
    return stringInDaysToTimestampInSeconds(
        dateToStringInDays(timestampInSecondsToDate(timestamp)));
  }

  public static Date stringInSecondsToDate(String s) {
    return stringToDate(s, sFormatterInSec);
  }

  public static Date timestampInSecondsToDate(long timestamp) {
    return new Date(timestamp * 1000);
  }

  public static String dateToStringInDays(Date date) {
    return sFormatterInDay.format(date);
  }

  public static String dateToStringInMinutes(Date date) {
    return sFormatterInMin.format(date);
  }

  /**
   * Convert a date time string (e.g., "2011-06-07 19:12:18") to a timestamp
   * in milliseconds (i.e., the number of milliseconds since January 1, 1970, 00:00:00 GMT.
   */
  public static long stringToTimestampInMilliseconds(String s, DateFormat formatter) {
    Date d = stringToDate(s, formatter);
    return d == null ? 0 : d.getTime();
  }

  /**
   * Convert a date time string (e.g., "2011-06-07 19:12:18") to a timestamp
   * in seconds (i.e., the number of seconds since January 1, 1970, 00:00:00 GMT.
   */
  public static long stringToTimestampInSeconds(String s, DateFormat formatter) {
    return stringToTimestampInMilliseconds(s, formatter) / 1000;
  }

  public static long stringInSecondsToTimestampInSeconds(String s) {
    return stringToTimestampInSeconds(s, sFormatterInSec);
  }

  public static long stringInDaysToTimestampInSeconds(String s) {
    return stringToTimestampInSeconds(s, sFormatterInDay);
  }

  public static long stringInMinutesToTimestampInSeconds(String s) {
    return stringToTimestampInSeconds(s, sFormatterInMin);
  }

  /**
   * Calculate a time decay penalty score in (0.3, 1).
   *
   * The demotion function used here is based on the exponential decay and
   * maps the value to interval (0.3, 1).
   *
   *     D(x) = 0.3 + 0.7 * (1 - 0.1)^delta(t)
   *
   * where 0.3 the lower bound of the penaly score, 0.7 is the initial value
   * and 0.1 (10%) is the percent decrease over a day, and delta(t) is the
   * days between a pin's creation time and now.
   *
   * Search "y = 0.3 + 0.7 * (1 - 0.1)^x" at google to visualize the function.
   *
   * Examples:
   *     D(0.0)   = 1.0
   *     D(0.5)   = 0.9640783086353595
   *     P(1.0)   = 0.9299999999999999
   *     P(2.0)   = 0.8670000000000000
   *     P(3.0)   = 0.810300000000000
   *     P(4.0)   = 0.7592699999999999
   *     P(5.0)   = 0.7133430000000001
   *     P(10.0)  = 0.54407490807
   *     P(20.0)  = 0.38510365821339854
   *     P(30.0)  = 0.32967381079265135
   *     P(365.0) = 0.300000000000000
   *
   * Args:
   *     eventTimestamp: the timestamp when the event took place.
   *     nowTimestamp: the current timestamp.
   *
   * Returns:
   *     The time decay penalty of the event.
   */
  public static double calculateTimeDecayPenalty(long eventTimestamp, long nowTimestamp) {
    if (nowTimestamp == 0) {
      return 1.0;
    }

    if (eventTimestamp == 0) {
      return 0.3;
    }

    // Calculate the days between the event and now.
    double days = (nowTimestamp - eventTimestamp) / 86400.0;
    return calculateTimeDecayPenalty(0.3, 0.7, 0.9, days);
  }

  /**
   * Calculate the arbitary time decay: y = base + boost * (param)^days
   */
  public static double calculateTimeDecayPenalty(double base, double boost,
                                                 double param, double days) {
    if (days <= 0.0) {
      return base + boost;
    }
    return base + boost * Math.pow(param, days);
  }

  /**
   * Return the current wall-clock time in nanoseconds.
   */
  public static long getNowTimestampInNanos() {
    return System.currentTimeMillis() * 1000 * 1000;
  }
}
