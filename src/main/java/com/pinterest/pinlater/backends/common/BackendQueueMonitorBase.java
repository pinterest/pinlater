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
package com.pinterest.pinlater.backends.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The base class of backend queue monitor. It implements a scheduled task used by the
 * PinLaterBackendBase for various types of job queue cleanup, including ACK timeouts and GC'ing
 * finished jobs. The detailed logic of the cleanup operations specific to each backend should be
 * implemented in the subclass. T is the type of datastore pool used by PinLaterBackendBase.
 */
public abstract class BackendQueueMonitorBase<T> implements Runnable {

  private final ImmutableMap<String, T> shardMap;
  private final int updateMaxSize;
  private final int maxAutoRetries;
  private final int logInterval;
  private final long jobClaimedTimeoutMillis;
  private final long jobSucceededGCTimeoutMillis;
  private final long jobFailedGCTimeoutMillis;
  protected final int numPriorityLevels;

  protected ImmutableMap<String, T> getShardMap() {
    return shardMap;
  }

  protected final int getUpdateMaxSize() {
    return updateMaxSize;
  }

  protected final int getMaxAutoRetries() {
    return maxAutoRetries;
  }

  public int getLogInterval() {
    return logInterval;
  }

  protected final long getJobClaimedTimeoutMillis() {
    return jobClaimedTimeoutMillis;
  }

  protected final long getJobSucceededGCTimeoutMillis() {
    return jobSucceededGCTimeoutMillis;
  }

  protected final long getJobFailedGCTimeoutMillis() {
    return jobFailedGCTimeoutMillis;
  }

  public BackendQueueMonitorBase(ImmutableMap<String, T> shardMap,
                                 PropertiesConfiguration configuration) {
    this(shardMap,
        configuration.getInt("BACKEND_MONITOR_UPDATE_MAX_SIZE"),
        configuration.getInt("BACKEND_NUM_AUTO_RETRIES"),
        configuration.getInt("MONITOR_LOG_INTERVAL", 1),
        TimeUnit.SECONDS.toMillis(
            configuration.getInt("BACKEND_MONITOR_JOB_CLAIMED_TIMEOUT_SECONDS")),
        TimeUnit.HOURS.toMillis(configuration.getInt(
            "BACKEND_MONITOR_JOB_SUCCEEDED_GC_TTL_HOURS")),
        TimeUnit.HOURS.toMillis(configuration.getInt(
            "BACKEND_MONITOR_JOB_FAILED_GC_TTL_HOURS")),
        configuration.getInt("NUM_PRIORITY_LEVELS"));
  }

  @VisibleForTesting
  public BackendQueueMonitorBase(ImmutableMap<String, T> shardMap,
                                 int updateMaxSize, int maxAutoRetries, int logInterval,
                                 long jobClaimedTimeoutMillis, long jobSucceededGCTimeoutMillis,
                                 long jobFailedGCTimeoutMillis,
                                 int numPriorityLevels) {
    this.shardMap = Preconditions.checkNotNull(shardMap);
    this.updateMaxSize = updateMaxSize;
    this.maxAutoRetries = maxAutoRetries;
    this.logInterval = logInterval;
    this.jobClaimedTimeoutMillis = jobClaimedTimeoutMillis;
    this.jobSucceededGCTimeoutMillis = jobSucceededGCTimeoutMillis;
    this.jobFailedGCTimeoutMillis = jobFailedGCTimeoutMillis;
    this.numPriorityLevels = numPriorityLevels;
  }

  @Override
  public void run() {
    final long runStartMillis = System.currentTimeMillis();
    for (ImmutableMap.Entry<String, T> shard : shardMap.entrySet()) {
      jobMonitorImpl(runStartMillis, shard, maxAutoRetries);
    }
  }

  protected abstract void jobMonitorImpl(long runStartMillis,
                                         Map.Entry<String, T> shard,
                                         int numAutoRetries);
}
