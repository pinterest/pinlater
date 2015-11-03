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
package com.pinterest.pinlater.commons.ostrich;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.pinterest.pinlater.commons.util.TimeUtils;
import com.twitter.common.base.MorePreconditions;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.FutureEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * An event listener for tracking success times and failure exceptions for futures.
 * @param <T> The inner class of the future.
 */
public class StatTrackingEventListener<T> implements FutureEventListener<T> {

  private static final Logger LOG = LoggerFactory.getLogger(StatTrackingEventListener.class);

  private long startTime;
  private String statPrefix;
  private String info;
  private boolean logError;
  private Predicate<Throwable> ignoreExceptionPredicate;
  private Map<String, String> tags;

  /**
   * An event listener for tracking success and failure times and exceptions.
   * @param startTime Obtained from TimeUtils.millisTime() ONLY!
   * @param prefix The prefix of the stat.
   * @param info The info will be logged when an exception happens
   * @param logError true if errors should be logged
   * @param tags a map of tags to be appended onto the stats
   */
  public StatTrackingEventListener(
      long startTime,
      String prefix,
      String info,
      boolean logError,
      Map<String, String> tags) {
    this.startTime = startTime;
    this.statPrefix = MorePreconditions.checkNotBlank(prefix);
    this.info = info;
    this.logError = logError;
    ignoreExceptionPredicate = Predicates.alwaysFalse();
    appendTags(tags);
  }

  /**
   * An event listener for tracking success and failure times and exceptions.
   * @param startTime Obtained from TimeUtils.millisTime() ONLY!
   * @param prefix The prefix of the stat.
   * @param info The info will be logged when an exception happens
   * @param logError true if errors should be logged
   */
  public StatTrackingEventListener(
      long startTime,
      String prefix,
      String info,
      boolean logError) {
    this(startTime, prefix, info, logError, null);
  }

  /**
   * An event listener for tracking success and failure times and exceptions.
   * @param startTime Obtained from TimeUtils.millisTime() ONLY!
   * @param prefix The prefix of the stat.
   * @param logError true if errors should be logged
   */
  public StatTrackingEventListener(long startTime, String prefix, boolean logError) {
    this(startTime, prefix, prefix, logError, null);
  }

  /**
   * An event listener for tracking success and failure times and exceptions.
   * The error message won't be logged
   * @param startTime Obtained from TimeUtils.millisTime() ONLY!
   * @param prefix The prefix of the stat.
   */
  public StatTrackingEventListener(long startTime, String prefix) {
    this(startTime, prefix, null, false, null);
  }

  @Override
  public void onSuccess(T t) {
    // Counts can be derived from the metric's count.
    Stats.addMetric(
        String.format("%s.success", statPrefix),
        (int) (TimeUtils.millisTime() - startTime));
    String tagsString = getTagsString();
    if (!Strings.isNullOrEmpty(tagsString)) {
      Stats.addMetric(
          String.format("%s.success.withtags%s", statPrefix, tagsString),
          (int) (TimeUtils.millisTime() - startTime));
    }
  }

  @Override
  public void onFailure(Throwable throwable) {
    boolean isExceptionIgnored = ignoreExceptionPredicate.apply(throwable);
    if (logError && !isExceptionIgnored) {
      int duration = (int) (TimeUtils.millisTime() - startTime);
      String message = String.format("%s; Duration: %s; %s", statPrefix, duration, info);
      LOG.error(message, throwable);
    }

    String exceptionStatsString = isExceptionIgnored ? "ignoredexception" : "exception";
    Stats.incr(String.format("%s.%s%s", statPrefix, exceptionStatsString, getTagsString()));
    Stats.incr(
        String.format(
            "%s.%s.%s%s",
            statPrefix,
            exceptionStatsString,
            throwable.getClass().getSimpleName(),
            getTagsString()));
  }

  /**
   * Append a map of tag names and tag values to the end of the tags string.
   * The tags string will be attached onto counters/metrics
   * @param newTags a map of tag names and tag values
   */
  public StatTrackingEventListener appendTags(Map<String, String> newTags) {
    if (newTags == null || newTags.isEmpty()) {
      return this;
    }
    if (tags == null) {
      tags = Maps.newLinkedHashMap();
    }
    tags.putAll(newTags);
    return this;
  }

  /**
   * Get a tag string from tags map to append to the stat name
   */
  @VisibleForTesting
  public String getTagsString() {
    if (tags == null || tags.isEmpty()) {
      return "";
    }
    StringBuilder tagBuilder = new StringBuilder();
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      if (tag.getValue() != null && !tag.getValue().isEmpty()) {
        tagBuilder
            .append(" ")
            .append(tag.getKey())
            .append("=")
            .append(tag.getValue());
      }
    }
    return tagBuilder.toString();
  }

  public StatTrackingEventListener setIgnoreExceptionPredicate(Predicate<Throwable> predicate) {
    if (predicate != null) {
      ignoreExceptionPredicate = predicate;
    } else {
      LOG.error("Null ignoreExceptionPredicate! Replaced with Predicates.alwaysFalse()");
      ignoreExceptionPredicate = Predicates.alwaysFalse();
    }
    return this;
  }
}
