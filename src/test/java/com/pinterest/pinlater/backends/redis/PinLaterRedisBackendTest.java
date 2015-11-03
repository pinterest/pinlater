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
import com.pinterest.pinlater.backends.common.PinLaterBackendBaseTest;
import com.pinterest.pinlater.backends.common.PinLaterJobDescriptor;
import com.pinterest.pinlater.backends.common.PinLaterTestUtils;
import com.pinterest.pinlater.thrift.PinLaterDequeueRequest;
import com.pinterest.pinlater.thrift.PinLaterDequeueResponse;
import com.pinterest.pinlater.thrift.PinLaterEnqueueRequest;
import com.pinterest.pinlater.thrift.PinLaterEnqueueResponse;
import com.pinterest.pinlater.thrift.PinLaterGetJobCountRequest;
import com.pinterest.pinlater.thrift.PinLaterJob;
import com.pinterest.pinlater.thrift.PinLaterJobAckInfo;
import com.pinterest.pinlater.thrift.PinLaterJobAckRequest;
import com.pinterest.pinlater.thrift.PinLaterJobInfo;
import com.pinterest.pinlater.thrift.PinLaterJobState;
import com.pinterest.pinlater.thrift.PinLaterLookupJobRequest;
import com.pinterest.pinlater.thrift.PinLaterScanJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterScanJobsResponse;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

@RunWith(JUnit4.class)
public class PinLaterRedisBackendTest extends PinLaterBackendBaseTest<PinLaterRedisBackend> {

  private static String QUEUE_NAME;
  private static PinLaterRedisBackend backend;
  private static PropertiesConfiguration configuration;
  private static final String LOCALHOST = "localhost";
  private static final int REDIS_PORT = 6379;

  @Override
  protected String getQueueName() {
    return QUEUE_NAME;
  }

  @Override
  protected PinLaterRedisBackend getBackend() {
    return backend;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    QUEUE_NAME = "pinlater_redis_backend_test";
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
    if (LocalRedisChecker.isRunning(REDIS_PORT)) {
      // Clean up redis.
      backend.cleanUpAllShards().get();
    }

    // Create the test queue.
    backend.createQueue(QUEUE_NAME).get();
  }

  @After
  public void afterTest() {
    if (LocalRedisChecker.isRunning(REDIS_PORT)) {
      // Clean up redis.
      backend.cleanUpAllShards().get();
    }
  }

  /*
   * Redis backend ensure that each custom status should not exceed size
   * RedisBackendUtils.CUSTOM_STATUS_SIZE_BYTES.
   */
  @Test
  public void testCustomStatusTruncatesWhenTooLong() {
    // Enqueue a job with custom stats "*".
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(QUEUE_NAME);
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("job_body".getBytes()));
    job.setCustomStatus(Strings.repeat("*", RedisBackendUtils.CUSTOM_STATUS_SIZE_BYTES * 2));
    enqueueRequest.addToJobs(job);
    PinLaterEnqueueResponse enqueueResponse = backend.enqueueJobs(enqueueRequest).get();
    String jobDesc = enqueueResponse.getJobDescriptors().get(0);
    Assert.assertEquals(1, (int) backend.getJobCount(
        new PinLaterGetJobCountRequest(QUEUE_NAME, PinLaterJobState.PENDING)).get());
    // Look up the job to make sure that the custom status was truncated when we appended too much.
    PinLaterLookupJobRequest lookupJobRequest = new PinLaterLookupJobRequest();
    lookupJobRequest.addToJobDescriptors(jobDesc);
    Map<String, PinLaterJobInfo> jobInfoMap = backend.lookupJobs(lookupJobRequest).get();
    String customStatus = jobInfoMap.get(jobDesc).getCustomStatus();
    Assert.assertEquals(Strings.repeat("*", RedisBackendUtils.CUSTOM_STATUS_SIZE_BYTES),
        customStatus);

    // Dequeue the job.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(QUEUE_NAME, 1);
    PinLaterDequeueResponse dequeueResponse = backend.dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(1, dequeueResponse.getJobsSize());
    Assert.assertTrue(dequeueResponse.getJobs().containsKey(jobDesc));
    Assert.assertEquals(1, (int) backend.getJobCount(
        new PinLaterGetJobCountRequest(QUEUE_NAME, PinLaterJobState.IN_PROGRESS)).get());

    // Ack the job with new custom status.
    PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(QUEUE_NAME);
    PinLaterJobAckInfo ackInfo = new PinLaterJobAckInfo(jobDesc);
    ackInfo.setAppendCustomStatus(
        Strings.repeat("-", RedisBackendUtils.CUSTOM_STATUS_SIZE_BYTES * 2));
    ackRequest.addToJobsSucceeded(ackInfo);
    backend.ackDequeuedJobs(ackRequest).get();
    Assert.assertEquals(1, (int) backend.getJobCount(
        new PinLaterGetJobCountRequest(QUEUE_NAME, PinLaterJobState.SUCCEEDED)).get());
    // Look up the job to make sure that the custom status was overwritten and truncated.
    lookupJobRequest = new PinLaterLookupJobRequest();
    lookupJobRequest.addToJobDescriptors(jobDesc);
    jobInfoMap = backend.lookupJobs(lookupJobRequest).get();
    customStatus = jobInfoMap.get(jobDesc).getCustomStatus();
    Assert.assertEquals(Strings.repeat("-", RedisBackendUtils.CUSTOM_STATUS_SIZE_BYTES),
        customStatus);
  }

  @Test
  public void testGetQueueNames() {
    Assert.assertTrue(backend.getQueueNames().get().contains(QUEUE_NAME));

    // Remove the test queue, create 3 more queues.
    backend.cleanUpAllShards().get();
    Set<String> testQueueNames = Sets.newHashSet();
    for (int i = 0; i < 3; i++) {
      String queueName = QUEUE_NAME + i;
      testQueueNames.add(queueName);
      backend.createQueue(queueName).get();
    }
    Set<String> queueNames = backend.getQueueNames().get();
    Assert.assertFalse(queueNames.contains(QUEUE_NAME));
    Assert.assertTrue(queueNames.containsAll(testQueueNames));
  }

  @Test
  public void testDeleteQueueNotDeleteAllZsets() {
    // Make sure we only delete all the jobs in the pending and in_progress queues.
    Assert.assertTrue(backend.getQueueNames().get().contains(QUEUE_NAME));

    // Generate 1 failed job.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("job_body".getBytes()));
    enqueueRequest.addToJobs(job);
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    List<String> jobDescriptors = enqueueResponse.getJobDescriptors();
    Assert.assertEquals(1, enqueueResponse.getJobDescriptorsSize());

    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    getBackend().dequeueJobs("test", dequeueRequest).get();
    PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(getQueueName());
    ackRequest.addToJobsFailed(new PinLaterJobAckInfo(jobDescriptors.get(0)));
    getBackend().ackDequeuedJobs(ackRequest).get();
    getBackend().dequeueJobs("test", dequeueRequest).get();
    getBackend().ackDequeuedJobs(ackRequest).get();

    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.FAILED));

    // Generate 1 succeeded job.
    enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    jobDescriptors = enqueueResponse.getJobDescriptors();
    Assert.assertEquals(1, enqueueResponse.getJobDescriptorsSize());

    getBackend().dequeueJobs("test", dequeueRequest).get();
    ackRequest = new PinLaterJobAckRequest(getQueueName());
    ackRequest.addToJobsSucceeded(new PinLaterJobAckInfo(jobDescriptors.get(0)));
    getBackend().ackDequeuedJobs(ackRequest).get();

    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.SUCCEEDED));

    // Generate 1 in progress job.
    enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(1, enqueueResponse.getJobDescriptorsSize());
    getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    // Generate 1 pending job.
    enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(1, enqueueResponse.getJobDescriptorsSize());
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));

    getBackend().deleteQueue(getQueueName()).get();
    Assert.assertFalse(getBackend().getQueueNames().get().contains(getQueueName()));
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.FAILED));
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.SUCCEEDED));
    Assert.assertEquals(0, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));
    Assert.assertEquals(0, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));
  }

  @Test
  public void testDequeueJobHashEvicted() {
    // Ensure that redis backend handles gracefully when the hash of the job to dequeue has been
    // evicted but job id is still in the queue.
    Assert.assertTrue(getBackend().getQueueNames().get().contains(getQueueName()));

    // Enqueue 3 jobs.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 3; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(("job_body_" + i).getBytes()));
      enqueueRequest.addToJobs(job);
    }
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(3, enqueueResponse.getJobDescriptorsSize());

    // Remove the job hash for the first job.
    getBackend().removeJobHash(enqueueResponse.getJobDescriptors().get(0)).get();

    // Dequeue 3 jobs. The first job will not be dequeued.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(QUEUE_NAME, 3);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(2, dequeueResponse.getJobsSize());

    // The first job will still be in the in progress queue because LUA script moves it from pending
    // queue to in progress queue. It will be remove by queue monitor.
    Assert.assertEquals(3, (int) getBackend().getJobCount(
        new PinLaterGetJobCountRequest(getQueueName(), PinLaterJobState.IN_PROGRESS)).get());
    Assert.assertEquals(0, (int) getBackend().getJobCount(
        new PinLaterGetJobCountRequest(getQueueName(), PinLaterJobState.SUCCEEDED)).get());
    Assert.assertEquals(0, (int) getBackend().getJobCount(
        new PinLaterGetJobCountRequest(getQueueName(), PinLaterJobState.PENDING)).get());
    Assert.assertEquals(0, (int) getBackend().getJobCount(
        new PinLaterGetJobCountRequest(getQueueName(), PinLaterJobState.FAILED)).get());
  }

  @Test
  public void testAckJobHashEvicted() {
    // Ensure that redis backend handles gracefully when the hash of the job to ack has been evicted
    // but job id is still in the queue.
    Assert.assertTrue(getBackend().getQueueNames().get().contains(getQueueName()));

    // Enqueue 3 jobs.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 3; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(("job_body_" + i).getBytes()));
      enqueueRequest.addToJobs(job);
    }
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(3, enqueueResponse.getJobDescriptorsSize());

    // Dequeue 3 jobs.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 3);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(3, dequeueResponse.getJobsSize());

    // Remove the job hash for the first job.
    getBackend().removeJobHash(enqueueResponse.getJobDescriptors().get(0)).get();

    // Ack the 3 jobs. all of them will be in the succeeded queue. Though the job hash for the first
    // job has been evicted, we will silently ignore it when acking.
    PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(QUEUE_NAME);
    for (int i = 0; i < 3; i++) {
      PinLaterJobAckInfo ackInfo = new PinLaterJobAckInfo(
          enqueueResponse.getJobDescriptors().get(i));
      ackRequest.addToJobsSucceeded(ackInfo);
      getBackend().ackDequeuedJobs(ackRequest).get();
    }

    Assert.assertEquals(3, (int) getBackend().getJobCount(
        new PinLaterGetJobCountRequest(getQueueName(), PinLaterJobState.SUCCEEDED)).get());
    Assert.assertEquals(0, (int) getBackend().getJobCount(
        new PinLaterGetJobCountRequest(getQueueName(), PinLaterJobState.IN_PROGRESS)).get());
    Assert.assertEquals(0, (int) getBackend().getJobCount(
        new PinLaterGetJobCountRequest(getQueueName(), PinLaterJobState.PENDING)).get());
    Assert.assertEquals(0, (int) getBackend().getJobCount(
        new PinLaterGetJobCountRequest(getQueueName(), PinLaterJobState.FAILED)).get());
  }

  @Test
  public void testScanJobHashEvicted() {
    // Ensure that redis backend handles gracefully when the hash of the job to scan has been
    // evicted but job id is still in the queue.
    Assert.assertTrue(getBackend().getQueueNames().get().contains(QUEUE_NAME));

    // Enqueue 3 jobs.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 3; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(("job_body_" + i).getBytes()));
      enqueueRequest.addToJobs(job);
    }
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(3, enqueueResponse.getJobDescriptorsSize());

    // Dequeue 3 jobs.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(QUEUE_NAME, 3);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(3, dequeueResponse.getJobsSize());

    // Remove the job hash for the first job.
    getBackend().removeJobHash(enqueueResponse.getJobDescriptors().get(0)).get();

    // Scan in progress queue. It will return 2 jobs. The one without job hash should not be
    // returned.
    PinLaterScanJobsRequest scanJobsRequest = new PinLaterScanJobsRequest();
    scanJobsRequest.setQueueName(getQueueName());
    scanJobsRequest.setJobState(PinLaterJobState.IN_PROGRESS);
    PinLaterScanJobsResponse scanJobsResponse = getBackend().scanJobs(scanJobsRequest).get();
    Assert.assertEquals(2, scanJobsResponse.getScannedJobs().size());
    for (PinLaterJobInfo jobInfo : scanJobsResponse.getScannedJobs()) {
      Assert.assertNotEquals(jobInfo.getJobDescriptor(),
          enqueueResponse.getJobDescriptors().get(0));
    }
  }

  @Test
  public void testLookupJobHashEvicted() {
    // Ensure that redis backend does not include the job in the returned map when the hash of the
    // job to look up has been evicted.
    Assert.assertTrue(getBackend().getQueueNames().get().contains(QUEUE_NAME));

    // Enqueue 2 job.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 2; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(("job_body").getBytes()));
      enqueueRequest.addToJobs(job);
    }
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(2, enqueueResponse.getJobDescriptorsSize());

    // Remove the job hash for the first job.
    getBackend().removeJobHash(enqueueResponse.getJobDescriptors().get(0)).get();

    // Test lookupJob.
    PinLaterLookupJobRequest lookupJobRequest = new PinLaterLookupJobRequest();
    lookupJobRequest.setIncludeBody(true);
    lookupJobRequest.addToJobDescriptors(enqueueResponse.getJobDescriptors().get(0));
    lookupJobRequest.addToJobDescriptors(enqueueResponse.getJobDescriptors().get(1));
    Map<String, PinLaterJobInfo> jobInfoMap = getBackend().lookupJobs(lookupJobRequest).get();
    Assert.assertEquals(1, jobInfoMap.size());
    Assert.assertFalse(jobInfoMap.containsKey(enqueueResponse.getJobDescriptors().get(0)));
  }

  @Test
  public void testGetRandomShard() {
    // Check that getRandomShard() only returns healthy shard if ``healthyOnly`` is true.
    final String[] shardNames = {"0", "1"};
    final int[] ports = {REDIS_PORT, REDIS_PORT + 1};

    // Create shard map which contains two local ports.
    ImmutableMap<String, RedisPools> shardMap = new ImmutableMap.Builder<String, RedisPools>()
        .put(shardNames[0], new RedisPools(configuration, LOCALHOST, ports[0], false))
        .put(shardNames[1], new RedisPools(configuration, LOCALHOST, ports[1], false))
        .build();

    // Create health checker which monitors two local ports.
    final DummyHeartBeater[] heartBeaters = {
        new DummyHeartBeater(true), new DummyHeartBeater(false)
    };
    HealthChecker healthChecker = new HealthChecker("test");
    for (int i = 0; i < ports.length; i++) {
      healthChecker.addServer(
          LOCALHOST,
          ports[i],
          heartBeaters[i],
          10,      // consecutive failures
          10,      // consecutive successes
          10,      // ping interval secs
          true);   // is live initially
    }

    // Create random instances.
    // dummyRandom is used only to check that both shards are possible to be returned, while
    // realRandom is used in the case where only one shard should always be returned.
    DummyRandom dummyRandom = new DummyRandom();
    Random realRandom = new Random();

    // When both shards are healthy, they can both be returned by getRandomShard().
    Assert.assertTrue(healthChecker.isServerLive(LOCALHOST, ports[0]));
    Assert.assertTrue(healthChecker.isServerLive(LOCALHOST, ports[1]));

    Set<String> healthyShards = Sets.newHashSet();
    for (int i = 0; i < 2; i++) {
      dummyRandom.setNextInt(i);
      healthyShards.add(PinLaterRedisBackend.getRandomShard(
          shardMap, healthChecker, dummyRandom, true).getKey());
    }
    Assert.assertEquals(Sets.newHashSet(shardNames[0], shardNames[1]), healthyShards);

    // When only one shard is healthy, getRandomShard() can only return that one.
    healthChecker.simulatSecondsLapsed(200);
    Assert.assertTrue(healthChecker.isServerLive(LOCALHOST, ports[0]));
    Assert.assertFalse(healthChecker.isServerLive(LOCALHOST, ports[1]));
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(shardNames[0], PinLaterRedisBackend.getRandomShard(
          shardMap, healthChecker, realRandom, true).getKey());
    }

    // When both shards are unhealthy, getRandomShard() should return null.
    heartBeaters[0].heartBeatResult = false;
    healthChecker.simulatSecondsLapsed(200);
    Assert.assertFalse(healthChecker.isServerLive(LOCALHOST, ports[0]));
    Assert.assertFalse(healthChecker.isServerLive(LOCALHOST, ports[1]));
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(null, PinLaterRedisBackend.getRandomShard(
          shardMap, healthChecker, realRandom, true));
    }
  }

  @Test
  public void testDequeueOnlyShard() {
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    int numOfJobs = 100;
    for (int i = 0; i < numOfJobs; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("job_body".getBytes()));
      enqueueRequest.addToJobs(job);
    }
    getBackend().enqueueJobs(enqueueRequest).get();

    // In our test setup, we have 3 shards, the last one in dequeueOnly mode. Check that the
    // helper method does in fact return only the first two shards.
    ImmutableMap<String, RedisPools> enqueueableShards = getBackend().getEnqueueableShards();
    Assert.assertEquals(Sets.newHashSet("1", "2"), enqueueableShards.keySet());

    // Scan all jobs and check which shard they were enqueued into (all of them should be in the
    // set of enqueueable shards).
    PinLaterScanJobsRequest scanJobsRequest =
        new PinLaterScanJobsRequest(getQueueName(), PinLaterJobState.PENDING);
    List<PinLaterJobInfo> jobInfos = getBackend().scanJobs(scanJobsRequest).get().getScannedJobs();
    Assert.assertEquals(numOfJobs, jobInfos.size());
    Map<String, Integer> shardJobsCount = new HashMap<String, Integer>();
    for (PinLaterJobInfo jobInfo : jobInfos) {
      PinLaterJobDescriptor jobDescriptor = new PinLaterJobDescriptor(jobInfo.getJobDescriptor());
      String shardName = jobDescriptor.getShardName();
      Assert.assertTrue(enqueueableShards.keySet().contains(shardName));
      // Count the number of jobs in each shard
      if (!shardJobsCount.containsKey(shardName)) {
        shardJobsCount.put(shardName, 0);
      }
      shardJobsCount.put(shardName, shardJobsCount.get(shardName) + 1);
    }

    // Check if each shards gets roughly equal number of jobs (20% range)
    int averageJobs = numOfJobs / enqueueableShards.size();
    for (String shardName : enqueueableShards.keySet()) {
      Assert.assertTrue(Math.abs(shardJobsCount.get(shardName) - averageJobs) < numOfJobs * 0.2);
    }
  }

  @Test
  public void testDequeueUpdatesUpdatedAt() {
    // Enqueue 1 job.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    enqueueRequest.addToJobs(new PinLaterJob(ByteBuffer.wrap("job_body".getBytes())));
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    String jobDesc = enqueueResponse.getJobDescriptors().get(0);

    // Lookup the job to get the updated_at timestamp.
    PinLaterLookupJobRequest lookupJobRequest = new PinLaterLookupJobRequest();
    lookupJobRequest.addToJobDescriptors(jobDesc);
    PinLaterJobInfo jobInfo = getBackend().lookupJobs(lookupJobRequest).get().get(jobDesc);
    long oldUpdatedAt = jobInfo.getUpdatedAtTimestampMillis();

    // Dequeue 1 job.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    getBackend().dequeueJobs("test", dequeueRequest).get();

    // Lookup the job again and verify that the updated_at timestamp has been updated.
    jobInfo = getBackend().lookupJobs(lookupJobRequest).get().get(jobDesc);
    Assert.assertTrue(jobInfo.getUpdatedAtTimestampMillis() > oldUpdatedAt);
  }
}