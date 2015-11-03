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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class MySQLHealthMonitorTest {

  @Test
  public void testBasicFunctionality() throws Exception {
    MySQLHealthMonitor healthMonitor =
        new MySQLHealthMonitor(Sets.newHashSet("1", "2", "3"), 10, 7, 100);
    Assert.assertTrue(healthMonitor.isHealthy("1"));

    // Till we reach 10 samples, isHealthy should be true.
    for (int i = 0; i < 9; i++) {
      healthMonitor.recordSample("1", false);
    }
    Assert.assertTrue(healthMonitor.isHealthy("1"));

    // One more sample should take shard 1 to unhealthy state.
    healthMonitor.recordSample("1", true);
    Assert.assertFalse(healthMonitor.isHealthy("1"));

    // Other two shards are still healthy though.
    Assert.assertTrue(healthMonitor.isHealthy("2"));
    Assert.assertTrue(healthMonitor.isHealthy("3"));

    // Shard 1 health should be restored after 100ms.
    Thread.sleep(100);
    Assert.assertTrue(healthMonitor.isHealthy("1"));

    // Add 4 failures + 6 successes to shard 2, which should make it unhealthy.
    for (int i = 0; i < 10; i++) {
      healthMonitor.recordSample("2", i >= 4);
    }
    Assert.assertFalse(healthMonitor.isHealthy("2"));

    // Shard 2 health should be restored after 100ms.
    Thread.sleep(100);
    Assert.assertTrue(healthMonitor.isHealthy("2"));

    // Repeating above test should yield same result now.
    for (int i = 0; i < 10; i++) {
      healthMonitor.recordSample("2", i >= 4);
    }
    Assert.assertFalse(healthMonitor.isHealthy("2"));
  }

  @Test
  public void testNonExistentShardId() {
    MySQLHealthMonitor healthMonitor =
        new MySQLHealthMonitor(Sets.newHashSet("1", "2", "3"), 10, 7, 100);
    Assert.assertFalse(healthMonitor.isHealthy("5"));
  }

  @Test
  public void testUpdateHealthMap() {
    MySQLHealthMonitor healthMonitor =
        new MySQLHealthMonitor(Sets.newHashSet("1", "2", "3"), 10, 7, 100);

    for (int i = 0; i < 10; i++) {
      healthMonitor.recordSample("1", false);
      healthMonitor.recordSample("2", false);
    }

    // Start with three shards, 1 & 2 are unhealthy and 3 is healthy
    Assert.assertFalse(healthMonitor.isHealthy("1"));
    Assert.assertFalse(healthMonitor.isHealthy("2"));
    Assert.assertTrue(healthMonitor.isHealthy("3"));

    Map<String, Boolean> updateMap = Maps.newHashMap();

    // No change to shard 1, remove shard 3 and create two new shards (reuse shard ID 2)
    updateMap.put("1", false);
    updateMap.put("2", true);
    updateMap.put("4", true);

    healthMonitor.updateHealthMap(updateMap);

    // No change to shard 1, should remain unhealthy
    Assert.assertFalse(healthMonitor.isHealthy("1"));

    // New shards should be healthy
    Assert.assertTrue(healthMonitor.isHealthy("2"));
    Assert.assertTrue(healthMonitor.isHealthy("4"));

    // Checking non-existing shard should return false
    Assert.assertFalse(healthMonitor.isHealthy("3"));
  }
}
