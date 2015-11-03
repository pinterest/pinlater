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

import com.pinterest.pinlater.commons.healthcheck.DummyHeartBeater;
import com.pinterest.pinlater.commons.healthcheck.HealthChecker;
import com.pinterest.pinlater.backends.common.BackendQueueMonitorBaseTest;
import com.pinterest.pinlater.backends.common.PinLaterTestUtils;
import com.pinterest.pinlater.thrift.PinLaterDequeueRequest;
import com.pinterest.pinlater.thrift.PinLaterDequeueResponse;
import com.pinterest.pinlater.thrift.PinLaterEnqueueRequest;
import com.pinterest.pinlater.thrift.PinLaterEnqueueResponse;
import com.pinterest.pinlater.thrift.PinLaterJob;
import com.pinterest.pinlater.thrift.PinLaterJobState;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@RunWith(JUnit4.class)
public class RedisQueueMonitorTest extends BackendQueueMonitorBaseTest<PinLaterRedisBackend,
    RedisQueueMonitor> {

  private static final String QUEUE_NAME = "redis_queue_monitor_test";
  private static final String LOCALHOST = "localhost";
  private static final int REDIS_PORT = 6379;

  private static PinLaterRedisBackend backend;
  private static PropertiesConfiguration configuration;

  @Override
  protected String getQueueName() {
    return QUEUE_NAME;
  }

  @Override
  protected PinLaterRedisBackend getBackend() {
    return backend;
  }

  @Override
  protected RedisQueueMonitor createQueueMonitor(long jobClaimedTimeoutMillis,
                                                 long jobSucceededGCTimeoutMillis,
                                                 long jobFailedGCTimeoutMillis) {
    HealthChecker dummyHealthChecker = new HealthChecker("pinlater_redis_test");
    dummyHealthChecker.addServer(
        LOCALHOST,
        REDIS_PORT,
        new DummyHeartBeater(true),
        10,      // consecutive failures
        10,      // consecutive successes
        10,      // ping interval secs
        true);   // is live initially

    InputStream redisConfigStream = ClassLoader.getSystemResourceAsStream("redis.local.json");
    ImmutableMap<String, RedisPools> shardMap =
        RedisBackendUtils.buildShardMap(redisConfigStream, configuration);
    return new RedisQueueMonitor(shardMap,
        1000,  // update max size
        3,     // auto retries
        1,     // log interval
        jobClaimedTimeoutMillis,
        jobSucceededGCTimeoutMillis,
        jobFailedGCTimeoutMillis,
        3,     // max queue priority
        dummyHealthChecker
    );
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    // If there is no local Redis, skip this test.
    Assume.assumeTrue(LocalRedisChecker.isRunning(REDIS_PORT));

    // If there is no local Redis, skip this test.
    Assume.assumeTrue(LocalRedisChecker.isRunning(REDIS_PORT));

    configuration = new PropertiesConfiguration();
    try {
      configuration.load(ClassLoader.getSystemResourceAsStream("pinlater.redis.test.properties"));
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
    InputStream redisConfigStream = ClassLoader.getSystemResourceAsStream("redis.local.json");

    backend = new PinLaterRedisBackend(
        configuration, redisConfigStream, "localhost", System.currentTimeMillis());
  }

  @Before
  public void beforeTest() {
    // Clean up redis.
    backend.cleanUpAllShards().get();

    // Create the test queue.
    backend.createQueue(QUEUE_NAME).get();
  }

  @After
  public void afterTest() {
    if (!LocalRedisChecker.isRunning(REDIS_PORT)) {
      return;
    }

    // Clean up redis.
    backend.cleanUpAllShards().get();
  }

  @Test
  public void testTimeoutJobHashEvicted() throws InterruptedException {
    // Ensure that queue monitor handles gracefully when the hash of the job is evicted but job id
    // is still in the in progress queue.
    Assert.assertTrue(backend.getQueueNames().get().contains(QUEUE_NAME));

    // Enqueue 8 jobs, 4 of them with no retries, and 4 with 1 retry.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 8; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(new String("job_body_" + i).getBytes()));
      job.setNumAttemptsAllowed(i < 4 ? 1 : 2);
      enqueueRequest.addToJobs(job);
    }
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(8, enqueueResponse.getJobDescriptorsSize());

    // Dequeue all 8.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 8);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(8, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    Thread.sleep(TimeUnit.SECONDS.toMillis(1));

    // Remove the job hash for the first 2 jobs to simulate they are evicted. Queue monitor should
    // just silently remove them from in process queue without adding them to any other queues.
    for (int i = 0; i < 2; i++) {
      getBackend().removeJobHash(enqueueResponse.getJobDescriptors().get(i)).get();
    }

    // Run queue monitor configured with low job claimed timeout.
    createQueueMonitor(1, TimeUnit.HOURS.toMillis(1), TimeUnit.HOURS.toMillis(1)).run();

    // After GC, 2 jobs should go to FAILED, 2 jobs(hash evicted) should disappear from any queue,
    // and remaining 4 should go to SUCCEEDED.
    Assert.assertEquals(2, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.FAILED));
    Assert.assertEquals(4, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));
  }
}
