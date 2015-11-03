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

import com.pinterest.pinlater.backends.common.PinLaterBackendBaseTest;
import com.pinterest.pinlater.backends.common.PinLaterJobDescriptor;
import com.pinterest.pinlater.thrift.ErrorCode;
import com.pinterest.pinlater.thrift.PinLaterDequeueRequest;
import com.pinterest.pinlater.thrift.PinLaterDequeueResponse;
import com.pinterest.pinlater.thrift.PinLaterEnqueueRequest;
import com.pinterest.pinlater.thrift.PinLaterEnqueueResponse;
import com.pinterest.pinlater.thrift.PinLaterException;
import com.pinterest.pinlater.thrift.PinLaterGetJobCountRequest;
import com.pinterest.pinlater.thrift.PinLaterJob;
import com.pinterest.pinlater.thrift.PinLaterJobAckInfo;
import com.pinterest.pinlater.thrift.PinLaterJobAckRequest;
import com.pinterest.pinlater.thrift.PinLaterJobInfo;
import com.pinterest.pinlater.thrift.PinLaterJobState;
import com.pinterest.pinlater.thrift.PinLaterLookupJobRequest;
import com.pinterest.pinlater.thrift.PinLaterScanJobsRequest;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(JUnit4.class)
public class PinLaterMySQLBackendTest extends PinLaterBackendBaseTest<PinLaterMySQLBackend> {

  private static String QUEUE_NAME;
  private static PinLaterMySQLBackend backend;

  @Override
  protected String getQueueName() {
    return QUEUE_NAME;
  }

  @Override
  protected PinLaterMySQLBackend getBackend() {
    return backend;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    QUEUE_NAME = "pinlater_mysql_backend_test";
    // If there is no local MySQL, skip this test.
    boolean isLocalMySQLRunning = LocalMySQLChecker.isRunning();
    Assume.assumeTrue(isLocalMySQLRunning);
    PropertiesConfiguration configuration = new PropertiesConfiguration();
    try {
      configuration.load(ClassLoader.getSystemResourceAsStream("pinlater.test.properties"));
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
    System.setProperty("backend_config", "mysql.local.json");

    backend = new PinLaterMySQLBackend(
        configuration, "localhost", System.currentTimeMillis());
  }

  @Before
  public void beforeTest() {
    // Delete the test queue, if it exists already, for some reason.
    backend.deleteQueue(QUEUE_NAME).get();

    // Now create it.
    backend.createQueue(QUEUE_NAME).get();
  }

  @After
  public void afterTest() {
    if (!LocalMySQLChecker.isRunning()) {
      return;
    }

    // Clean up the test queue.
    backend.deleteQueue(QUEUE_NAME).get();
  }

  /*
   * MySQL backend ensure that the custom status stored in db should not exceed
   * ``CUSTOM_STATUS_SIZE_BYTES`` as specified in MySQLQueries.java.
   */
  @Test
  public void testCustomStatusTruncatesWhenTooLong() {
    // Enqueue a job with custom stats "*".
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(QUEUE_NAME);
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("job_body".getBytes()));
    job.setCustomStatus("*");
    enqueueRequest.addToJobs(job);
    PinLaterEnqueueResponse enqueueResponse = backend.enqueueJobs(enqueueRequest).get();
    String jobDesc = enqueueResponse.getJobDescriptors().get(0);

    // Dequeue the job.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(QUEUE_NAME, 1);
    PinLaterDequeueResponse dequeueResponse = backend.dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(1, dequeueResponse.getJobsSize());
    Assert.assertTrue(dequeueResponse.getJobs().containsKey(jobDesc));
    Assert.assertEquals(1, (int) backend.getJobCount(
        new PinLaterGetJobCountRequest(QUEUE_NAME, PinLaterJobState.IN_PROGRESS)).get());

    // Ack the job, appending to the custom status size with hyphens so that it'll overflow.
    PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(QUEUE_NAME);
    PinLaterJobAckInfo ackInfo = new PinLaterJobAckInfo(jobDesc);
    ackInfo.setAppendCustomStatus(Strings.repeat("-", MySQLQueries.CUSTOM_STATUS_SIZE_BYTES));
    ackRequest.addToJobsSucceeded(ackInfo);
    backend.ackDequeuedJobs(ackRequest).get();
    Assert.assertEquals(1, (int) backend.getJobCount(
        new PinLaterGetJobCountRequest(QUEUE_NAME, PinLaterJobState.SUCCEEDED)).get());

    // Look up the job to make sure that the custom status was truncated when we appended too much.
    PinLaterLookupJobRequest lookupJobRequest = new PinLaterLookupJobRequest();
    lookupJobRequest.setIncludeBody(true);
    lookupJobRequest.addToJobDescriptors(jobDesc);
    Map<String, PinLaterJobInfo> jobInfoMap = backend.lookupJobs(lookupJobRequest).get();
    PinLaterJobInfo jobInfo = jobInfoMap.get(jobDesc);
    String truncatedCustomStatus = jobInfo.getCustomStatus();
    Assert.assertEquals("*" + Strings.repeat("-", MySQLQueries.CUSTOM_STATUS_SIZE_BYTES - 1),
        truncatedCustomStatus);
  }

  @Test
  public void testQueueNameTooLong() {
    try {
      String longQueueName = Strings.repeat("a", MySQLBackendUtils.MYSQL_MAX_DB_NAME_LENGTH + 1);
      backend.createQueue(longQueueName).get();
    } catch (Exception e) {
      if (e instanceof PinLaterException) {
        Assert.assertEquals(ErrorCode.QUEUE_NAME_TOO_LONG, ((PinLaterException) e).getErrorCode());
        return;
      }
    }
    Assert.fail();
  }

  @Test
  public void testQueueDoesNotExistDequeue() {
    try {
      PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest("nonexistent_queue", 1);
      backend.dequeueJobs("test", dequeueRequest).get();
    } catch (Exception e) {
      if (e instanceof PinLaterException) {
        Assert.assertEquals(ErrorCode.QUEUE_NOT_FOUND, ((PinLaterException) e).getErrorCode());
        return;
      }
    }
    Assert.fail();
  }

  @Test
  public void testQueueDoesNotExistAckSuccess() {
    try {
      PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest("nonexistent_queue");
      PinLaterJobAckInfo ackInfo = new PinLaterJobAckInfo("nonexistent_queue:s1:p1:1");
      ackRequest.addToJobsSucceeded(ackInfo);
      backend.ackDequeuedJobs(ackRequest).get();
    } catch (Exception e) {
      if (e instanceof PinLaterException) {
        Assert.assertEquals(ErrorCode.QUEUE_NOT_FOUND, ((PinLaterException) e).getErrorCode());
        return;
      }
    }
    Assert.fail();
  }

  @Test
  public void testQueueDoesNotExistAckFail() {
    try {
      PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest("nonexistent_queue");
      PinLaterJobAckInfo ackInfo = new PinLaterJobAckInfo("nonexistent_queue:s1:p1:1");
      ackRequest.addToJobsFailed(ackInfo);
      backend.ackDequeuedJobs(ackRequest).get();
    } catch (Exception e) {
      if (e instanceof PinLaterException) {
        Assert.assertEquals(ErrorCode.QUEUE_NOT_FOUND, ((PinLaterException) e).getErrorCode());
        return;
      }
    }
    Assert.fail();
  }

  // TODO (klo): move this to PinLaterBackendBaseTest. Feel free to either merge it with
  // testGetJobCount() and leave this as a standalone test if that feels too cluttered.
  @Test
  public void testGetJobCountWithBody() {
    // Enqueue 10 jobs.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 10; i++) {
      PinLaterJob job;
      if (i % 2 == 0) {
        job = new PinLaterJob(ByteBuffer.wrap(("body_even_" + i).getBytes()));
      } else {
        job = new PinLaterJob(ByteBuffer.wrap(("body_odd_" + i).getBytes()));
      }
      enqueueRequest.addToJobs(job);
    }
    getBackend().enqueueJobs(enqueueRequest).get();

    // Count jobs that have bodies containing the string "even" (there should be 5 of them).
    PinLaterGetJobCountRequest getJobCountRequest =
        new PinLaterGetJobCountRequest(getQueueName(), PinLaterJobState.PENDING);
    getJobCountRequest.setBodyRegexToMatch(".*even.*");
    int jobCount = getBackend().getJobCount(getJobCountRequest).get();
    Assert.assertEquals(5, jobCount);
  }

  // TODO (klo): move this to PinLaterBackendBaseTest. Feel free to either merge it with
  // testScanJobs() and leave this as a standalone test if that feels too cluttered.
  @Test
  public void testScanJobsWithBody() {
    // Enqueue 10 jobs.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 10; i++) {
      PinLaterJob job;
      if (i % 2 == 0) {
        job = new PinLaterJob(ByteBuffer.wrap(("body_even_" + i).getBytes()));
      } else {
        job = new PinLaterJob(ByteBuffer.wrap(("body_odd_" + i).getBytes()));
      }
      enqueueRequest.addToJobs(job);
    }
    getBackend().enqueueJobs(enqueueRequest).get();

    // Scan jobs that have bodies containing the string "even" (there should be 5 of them). Note
    // that we have to grab the job descriptors first and then use lookupJobs on those in order
    // to get the actual job bodies.
    PinLaterScanJobsRequest scanJobsRequest =
        new PinLaterScanJobsRequest(getQueueName(), PinLaterJobState.PENDING);
    scanJobsRequest.setBodyRegexToMatch(".*even.*");
    List<PinLaterJobInfo> jobInfos = getBackend().scanJobs(scanJobsRequest).get().getScannedJobs();
    Assert.assertEquals(5, jobInfos.size());
    for (PinLaterJobInfo jobInfo : jobInfos) {
      String jobDescriptor = jobInfo.getJobDescriptor();
      PinLaterLookupJobRequest lookupJobRequest =
          new PinLaterLookupJobRequest(Arrays.asList(jobDescriptor));
      lookupJobRequest.setIncludeBody(true);
      byte[] body = getBackend().lookupJobs(lookupJobRequest).get().get(jobDescriptor).getBody();
      Assert.assertTrue(new String(body).contains("even"));
    }
  }

  /*
   * Enqueue 100 jobs to a set of shards, checks if all enqueueable shards actually get roughly
   * equal number of jobs
   */
  private void testEnqueueToShardsHelper(ImmutableSet<String> shards) {
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    int numOfJobs = 500;
    for (int i = 0; i < numOfJobs; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("job_body".getBytes()));
      enqueueRequest.addToJobs(job);
    }
    getBackend().enqueueJobs(enqueueRequest).get();

    // Scan all jobs and check which shard they were enqueued into (all of them should be in the
    // set of enqueueable shards).
    PinLaterScanJobsRequest scanJobsRequest =
        new PinLaterScanJobsRequest(getQueueName(), PinLaterJobState.PENDING);
    scanJobsRequest.setLimit(numOfJobs * 2);
    List<PinLaterJobInfo> jobInfos = getBackend().scanJobs(scanJobsRequest).get().getScannedJobs();
    Assert.assertEquals(numOfJobs, jobInfos.size());
    Map<String, Integer> shardJobsCount = new HashMap<String, Integer>();
    for (PinLaterJobInfo jobInfo : jobInfos) {
      PinLaterJobDescriptor jobDescriptor = new PinLaterJobDescriptor(jobInfo.getJobDescriptor());
      String shardName = jobDescriptor.getShardName();
      Assert.assertTrue(shards.contains(shardName));
      // Count the number of jobs in each shard
      if (!shardJobsCount.containsKey(shardName)) {
        shardJobsCount.put(shardName, 0);
      }
      shardJobsCount.put(shardName, shardJobsCount.get(shardName) + 1);
    }

    // Check if each shards gets roughly equal number of jobs (20% range)
    int averageJobs = numOfJobs / shards.size();
    for (String shardName : shards) {
      Assert.assertTrue(Math.abs(shardJobsCount.get(shardName) - averageJobs) < numOfJobs * 0.2);
    }
  }

  public void testDequeueOnlyShard() {
    // In our test setup, we have 3 shards, the last one in dequeueOnly mode. Check that the
    // helper method does in fact return only the first two shards.
    ImmutableMap<String, MySQLDataSources> enqueueableShards = getBackend().getEnqueueableShards();
    Assert.assertEquals(Sets.newHashSet(1, 2), enqueueableShards.keySet());
    testEnqueueToShardsHelper(enqueueableShards.keySet());
  }

  private static String TEST_SHARD_CONFIG =
      "    \"pinlaterlocaldb00%d\": {\n" +
          "        \"master\": {\n" +
          "            \"host\": \"localhost\",\n" +
          "            \"port\": 3306\n" +
          "        },\n" +
          "        \"slave\": {\n" +
          "            \"host\": \"localhost\",\n" +
          "            \"port\": 3306\n" +
          "        },\n" +
          "         \"user\": \"root\",\n" +
          "         \"passwd\": \"\"\n" +
          "    }";

  @Test
  public void testConfigUpdate() throws Exception {
    ImmutableMap<String, MySQLDataSources> enqueueableShards = getBackend().getEnqueueableShards();
    Assert.assertEquals(
        Sets.newHashSet("1", "1d1", "1d2", "2", "2d1", "2d2", "3", "3d1", "3d2"),
        getBackend().getShards());
    Assert.assertEquals(enqueueableShards.get("1"), enqueueableShards.get("1d1"));
    Assert.assertNull(enqueueableShards.get("3"));
    Assert.assertNull(enqueueableShards.get("3d1"));

    // We make three changes to the config: remove shard 2, make shard 3 enqueueable and add a
    // new shard 4 (It's dangerous to remove a "enqueuable" shard. In practice we need to make a
    // shard dequeueOnly and let it drain before remove it).
    String updatedConfigString = "{\n" +
        String.format(TEST_SHARD_CONFIG, 1) + ",\n" +
        String.format(TEST_SHARD_CONFIG, 3) + ",\n" +
        String.format(TEST_SHARD_CONFIG, 4) + "\n}";
    getBackend().processConfigUpdate(updatedConfigString.getBytes());
    ImmutableMap<String, MySQLDataSources> updatedEnqueueableShards =
        getBackend().getEnqueueableShards();
    Assert.assertEquals(
        Sets.newHashSet("1", "1d1", "1d2", "3", "3d1", "3d2", "4", "4d1", "4d2"),
        getBackend().getShards());
    Assert.assertEquals(enqueueableShards.get("1"), updatedEnqueueableShards.get("1"));
    Assert.assertFalse(updatedEnqueueableShards.get("3").isDequeueOnly());
    Assert.assertFalse(updatedEnqueueableShards.get("3d1").isDequeueOnly());

    testEnqueueToShardsHelper(updatedEnqueueableShards.keySet());

    // Add shard 2 back to the shard map to clean up all databases created in this test.
    updatedConfigString = "{\n" +
        String.format(TEST_SHARD_CONFIG, 1) + ",\n" +
        String.format(TEST_SHARD_CONFIG, 2) + ",\n" +
        String.format(TEST_SHARD_CONFIG, 3) + ",\n" +
        String.format(TEST_SHARD_CONFIG, 4) + "\n}";
    getBackend().processConfigUpdate(updatedConfigString.getBytes());
  }
}