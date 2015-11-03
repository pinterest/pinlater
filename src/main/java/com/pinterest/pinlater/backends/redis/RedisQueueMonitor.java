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
package com.pinterest.pinlater.backends.redis;

import com.pinterest.pinlater.backends.common.BackendQueueMonitorBase;
import com.pinterest.pinlater.commons.healthcheck.HealthChecker;
import com.pinterest.pinlater.thrift.PinLaterJobState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Function;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implements a scheduled task used by the PinLaterRedisBackend for various types of
 * job queue cleanup, including ACK timeouts and GC'ing finished jobs.
 */
public class RedisQueueMonitor extends BackendQueueMonitorBase<RedisPools> {

  private static final Logger LOG = LoggerFactory.getLogger(RedisQueueMonitor.class);

  private int logCount;
  private int numTimeoutDone;
  private int numTimeoutRetry;
  private long numSucceededGC;
  private long numFailedGC;
  private int numTimeoutEvict;
  private final HealthChecker healthChecker;

  public RedisQueueMonitor(ImmutableMap<String, RedisPools> shardMap,
                           PropertiesConfiguration configuration,
                           HealthChecker healthChecker) {
    super(shardMap, configuration);
    this.healthChecker = Preconditions.checkNotNull(healthChecker);
  }

  @VisibleForTesting
  public RedisQueueMonitor(ImmutableMap<String, RedisPools> shardMap,
                           int updateMaxSize, int maxAutoRetries, int logInterval,
                           long jobClaimedTimeoutMillis, long jobSucceededGCTimeoutMillis,
                           long jobFailedGCTimeoutMillis, int numPriorityLevels,
                           HealthChecker healthChecker) {
    super(shardMap, updateMaxSize, maxAutoRetries, logInterval, jobClaimedTimeoutMillis,
        jobSucceededGCTimeoutMillis, jobFailedGCTimeoutMillis, numPriorityLevels);
    this.healthChecker = Preconditions.checkNotNull(healthChecker);
  }

  @Override
  protected void jobMonitorImpl(long runStartMillis,
                                final Map.Entry<String, RedisPools> shard,
                                int numAutoRetries) {
    // Skip the shard if it is unhealthy.
    if (!healthChecker.isServerLive(
        shard.getValue().getHost(), shard.getValue().getPort())) {
      LOG.warn(String.format("Skipped monitoring shard %s because it is unhealthy.",
          shard.getKey()));
      return;
    }

    final double runStartTimeSeconds = runStartMillis / 1000.0;
    final double succeededGCTimeSeconds = (runStartMillis - getJobSucceededGCTimeoutMillis())
        / 1000.0;
    final double failedGCTimeSeconds = (runStartMillis - getJobFailedGCTimeoutMillis()) / 1000.0;
    final double timeoutTimeSeconds = (runStartMillis - getJobClaimedTimeoutMillis()) / 1000.0;

    try {
      RedisUtils.executeWithConnection(
          shard.getValue().getMonitorRedisPool(),
          new Function<Jedis, Void>() {
            @Override
            public Void apply(Jedis conn) {
              Set<String> queueNames = RedisBackendUtils.getQueueNames(conn, shard.getKey());
              for (String queueName : queueNames) {
                for (int priority = 1;
                     priority <= numPriorityLevels;
                     priority++) {
                  String pendingQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                      queueName, shard.getKey(), priority, PinLaterJobState.PENDING);
                  String inProgressQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                      queueName, shard.getKey(), priority, PinLaterJobState.IN_PROGRESS);
                  String succeededQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                      queueName, shard.getKey(), priority, PinLaterJobState.SUCCEEDED);
                  String failedQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                      queueName, shard.getKey(), priority, PinLaterJobState.FAILED);
                  String hashRedisKeyPrefix = RedisBackendUtils.constructHashRedisKeyPrefix(
                      queueName, shard.getKey());

                  // Handle timed out jobs.
                  List<String> keys = Lists.newArrayList(
                      inProgressQueueRedisKey,
                      hashRedisKeyPrefix,
                      pendingQueueRedisKey,
                      failedQueueRedisKey);
                  List<String> argv = Lists.newArrayList(
                      String.valueOf(timeoutTimeSeconds),
                      String.valueOf(getUpdateMaxSize()),
                      String.valueOf(runStartTimeSeconds));
                  Object nums = conn.eval(
                      RedisLuaScripts.MONITOR_TIMEOUT_UPDATE, keys, argv);
                  List<Object> tmp = (List<Object>) nums;
                  numTimeoutDone += Integer.valueOf((String) tmp.get(0));
                  numTimeoutRetry += Integer.valueOf((String) tmp.get(1));
                  numTimeoutEvict += Integer.valueOf((String) tmp.get(2));

                  // Succeeded job GC.
                  keys = Lists.newArrayList(succeededQueueRedisKey, hashRedisKeyPrefix);
                  argv = Lists.newArrayList(String.valueOf(succeededGCTimeSeconds),
                      String.valueOf(getUpdateMaxSize()));
                  numSucceededGC += (Long) conn.eval(
                      RedisLuaScripts.MONITOR_GC_DONE_JOBS, keys, argv);

                  // Failed job GC.
                  keys = Lists.newArrayList(failedQueueRedisKey, hashRedisKeyPrefix);
                  argv = Lists.newArrayList(String.valueOf(failedGCTimeSeconds),
                      String.valueOf(getUpdateMaxSize()));
                  numFailedGC += (Long) conn.eval(
                      RedisLuaScripts.MONITOR_GC_DONE_JOBS, keys, argv);

                  logCount++;
                  if (logCount % getLogInterval() == 0) {
                    LOG.info(String.format(
                        "JobQueueMonitor: "
                            + "Shard: %s Queue: %s Priority: %d Timeout Done: %d Timeout Retry: %d "
                            + "Succeeded GC: %d Failed GC: %d",
                        shard.getKey(), queueName, priority, numTimeoutDone, numTimeoutRetry,
                        numSucceededGC, numFailedGC));
                    if (numTimeoutEvict != 0) {
                      LOG.error(String.format(
                          "JobQueueMonitor: Shard: %s Queue: %s Priority: %d Timeout Evict: %d",
                          shard.getKey(), queueName, priority, numTimeoutEvict));
                    }
                    Stats.incr(queueName + "_timeout_done", numTimeoutDone);
                    Stats.incr(queueName + "_timeout_retry", numTimeoutRetry);
                    Stats.incr(queueName + "_timeout_evict", numTimeoutEvict);
                    Stats.incr(queueName + "_succeeded_gc", (int) numSucceededGC);
                    Stats.incr(queueName + "_failed_gc", (int) numFailedGC);
                    logCount = 0;
                    numTimeoutDone = 0;
                    numTimeoutRetry = 0;
                    numSucceededGC = 0;
                    numFailedGC = 0;
                    numTimeoutEvict = 0;
                  }
                }
              }
              return null;
            }
          });
    } catch (Exception e) {
      Stats.incr("PinLater.RedisQueueMonitor.errors." + e.getClass().getSimpleName());
      LOG.error("Exception in JobQueueMonitor task", e);
    }
  }
}
