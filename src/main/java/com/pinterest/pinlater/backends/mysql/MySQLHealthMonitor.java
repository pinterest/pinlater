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
package com.pinterest.pinlater.backends.mysql;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class that monitors the health of MySQL shards. The basic strategy is to keep a recent
 * window of "health" samples, i.e. whether a query was a success or failure, and use a
 * threshold to determine if the shard should be marked unhealthy. Once marked unhealthy,
 * status is reset after a short period of time, and the sampling process is repeated
 * from scratch.
 *
 * This class is thread safe.
 */
public class MySQLHealthMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLHealthMonitor.class);

  private final int windowSize;
  private final int healthySuccessThreshold;
  private final long unhealthyResetPeriodMillis;
  // Note: ShardState access should be explicitly synchronized.
  private final AtomicReference<ImmutableMap<String, ShardState>> shardHealthMapRef =
      new AtomicReference<ImmutableMap<String, ShardState>>();

  /**
   * Creates a MySQLHealthMonitor object with default configuration.
   *
   * @param shardNames set of MySQL shard ids.
   */
  public MySQLHealthMonitor(Set<String> shardNames) {
    this(shardNames,
        100,                             // window size
        75,                              // healthy success threshold
        TimeUnit.SECONDS.toMillis(60));  // health reset period
  }

  /**
   * Creates a MySQLHealthMonitor object.
   *
   * @param shardNames set of MySQL shard names.
   * @param windowSize number of recent health samples to keep per shard.
   * @param healthySuccessThreshold number of samples in windowSize that should successful for
   *                                marking a shard healthy.
   * @param unhealthyResetPeriodMillis period of time after which to reset unhealthy status.
   */
  public MySQLHealthMonitor(Set<String> shardNames,
                            int windowSize,
                            int healthySuccessThreshold,
                            long unhealthyResetPeriodMillis) {
    this.windowSize = windowSize;
    Preconditions.checkArgument(windowSize > 0);
    this.healthySuccessThreshold = healthySuccessThreshold;
    Preconditions.checkArgument(
        healthySuccessThreshold >= 0 && healthySuccessThreshold <= windowSize);
    this.unhealthyResetPeriodMillis = unhealthyResetPeriodMillis;
    Preconditions.checkArgument(unhealthyResetPeriodMillis > 0);

    ImmutableMap.Builder<String, ShardState> builder =
        new ImmutableMap.Builder<String, ShardState>();
    for (String shard : shardNames) {
      builder.put(shard, new ShardState(windowSize));
    }
    this.shardHealthMapRef.set(builder.build());
  }

  public synchronized void recordSample(String shardName, boolean success) {
    ImmutableMap<String, ShardState> shardHealthMap = shardHealthMapRef.get();

    ShardState shardState = shardHealthMap.get(shardName);
    if (shardState.healthSamples.remainingCapacity() == 0) {
      shardState.numSuccessesInWindow -= shardState.healthSamples.removeLast();
    }

    int successVal = success ? 1 : 0;
    shardState.numSuccessesInWindow += successVal;
    shardState.healthSamples.addFirst(successVal);
  }

  public synchronized boolean isHealthy(String shardName) {
    long currentTimeMillis = System.currentTimeMillis();
    ImmutableMap<String, ShardState> shardHealthMap = shardHealthMapRef.get();
    if (!shardHealthMap.containsKey(shardName)) {
      LOG.error("Health monitor map doesn't have shard name {}", shardName);
      return false;
    }

    ShardState shardState = shardHealthMap.get(shardName);
    if (shardState.isHealthy) {
      if (shardState.healthSamples.remainingCapacity() == 0
          && shardState.numSuccessesInWindow < healthySuccessThreshold) {
        LOG.info("Marking mysql shard {} unhealthy due to {} successes / 100",
            shardName, shardState.numSuccessesInWindow);
        shardState.isHealthy = false;
        shardState.lastUnhealthyTimestampMillis = currentTimeMillis;
      }
    } else if (currentTimeMillis
        >= shardState.lastUnhealthyTimestampMillis + unhealthyResetPeriodMillis) {
      LOG.info("Resetting health state for mysql shard {}", shardName);
      shardState.reset();
    }

    return shardState.isHealthy;
  }

  /**
   * Update health monitor map when the shard config is updated. This method doesn't need to be
   * synchronized because it is only called when config is updated, and ConfigFileWatcher ensures
   * that only one update is active.
   * @param shardNamesToChanged map of shard names, the value is true when it's a new/changed shard
   */
  public void updateHealthMap(Map<String, Boolean> shardNamesToChanged) {
    ImmutableMap.Builder<String, ShardState> builder =
        new ImmutableMap.Builder<String, ShardState>();
    for (String shard : shardNamesToChanged.keySet()) {
      if (!shardNamesToChanged.get(shard)) {
        builder.put(shard, shardHealthMapRef.get().get(shard));
      } else {
        builder.put(shard, new ShardState(windowSize));
      }
    }

    shardHealthMapRef.set(builder.build());
  }

  /**
   * Encapsulates per-MySQL-shard state kept track of by this class.
   */
  private static class ShardState {

    final LinkedBlockingDeque<Integer> healthSamples;
    int numSuccessesInWindow = 0;
    boolean isHealthy = true;
    long lastUnhealthyTimestampMillis = 0L;

    ShardState(int windowSize) {
      this.healthSamples = new LinkedBlockingDeque<Integer>(windowSize);
    }

    public void reset() {
      numSuccessesInWindow = 0;
      isHealthy = true;
      lastUnhealthyTimestampMillis = 0;
      healthSamples.clear();
    }
  }
}
