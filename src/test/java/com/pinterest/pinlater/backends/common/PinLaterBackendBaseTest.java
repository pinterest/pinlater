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

import com.pinterest.pinlater.PinLaterBackendBase;
import com.pinterest.pinlater.thrift.ErrorCode;
import com.pinterest.pinlater.thrift.PinLaterCheckpointJobRequest;
import com.pinterest.pinlater.thrift.PinLaterCheckpointJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterDeleteJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterDequeueMetadata;
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
import com.pinterest.pinlater.thrift.PinLaterRetryFailedJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterScanJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterScanJobsResponse;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * PinLater backend base test class, which implements all the basic tests that each backend should
 * pass.
 *
 * The test class for each backend should inherit this base class. It should do the initialization
 * of ``backend`` in ``beforeClass`` function, and do the backend cleanup in ``afterTest`` function.
 */
public abstract class PinLaterBackendBaseTest<T extends PinLaterBackendBase> {

  protected abstract String getQueueName();

  protected abstract T getBackend();

  /*
   * Check the custom status truncation behavior.
   *
   * This is tested by each backend separately because custom status truncation behavior is backend
   * specific.
   */
  @Test
  public abstract void testCustomStatusTruncatesWhenTooLong();

  @Test
  public void testBasicOperations() {
    // Enqueue one job.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("job_body".getBytes()));
    enqueueRequest.addToJobs(job);
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(1, enqueueResponse.getJobDescriptorsSize());
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));
    String jobDesc = enqueueResponse.getJobDescriptors().get(0);

    // Check that jobs are available to dequeue.
    Assert.assertTrue(areJobsAvailable(getQueueName()));

    // Test lookupJob.
    PinLaterLookupJobRequest lookupJobRequest = new PinLaterLookupJobRequest();
    lookupJobRequest.setIncludeBody(true);
    lookupJobRequest.addToJobDescriptors(jobDesc);
    Map<String, PinLaterJobInfo> jobInfoMap = getBackend().lookupJobs(lookupJobRequest).get();
    PinLaterJobInfo jobInfo = jobInfoMap.get(jobDesc);
    Assert.assertEquals(1, jobInfoMap.size());
    Assert.assertEquals(jobDesc, jobInfo.getJobDescriptor());
    Assert.assertEquals(PinLaterJobState.PENDING, jobInfo.getJobState());
    Assert.assertEquals(job.getNumAttemptsAllowed(), jobInfo.getAttemptsAllowed());
    Assert.assertEquals(job.getNumAttemptsAllowed(), jobInfo.getAttemptsRemaining());
    Assert.assertTrue(jobInfo.isSetCreatedAtTimestampMillis());
    Assert.assertTrue(jobInfo.isSetRunAfterTimestampMillis());
    Assert.assertTrue(jobInfo.isSetUpdatedAtTimestampMillis());
    long oldUpdatedAt = jobInfo.getUpdatedAtTimestampMillis();
    Assert.assertTrue(Strings.isNullOrEmpty(jobInfo.getClaimDescriptor()));
    Assert.assertTrue(Strings.isNullOrEmpty(jobInfo.getCustomStatus()));
    Assert.assertEquals("job_body", new String(jobInfo.getBody()));

    // Dequeue the job.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 10);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(1, dequeueResponse.getJobsSize());
    Assert.assertTrue(dequeueResponse.getJobs().containsKey(jobDesc));
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    // Ack the job.
    PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(getQueueName());
    ackRequest.addToJobsSucceeded(new PinLaterJobAckInfo(jobDesc));
    getBackend().ackDequeuedJobs(ackRequest).get();
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.SUCCEEDED));

    // areJobsAvailable should return false.
    Assert.assertFalse(areJobsAvailable(getQueueName()));

    // Test lookupJob.
    lookupJobRequest.setIncludeBody(false);
    jobInfoMap = getBackend().lookupJobs(lookupJobRequest).get();
    jobInfo = jobInfoMap.get(jobDesc);
    Assert.assertEquals(1, jobInfoMap.size());
    Assert.assertEquals(jobDesc, jobInfo.getJobDescriptor());
    Assert.assertEquals(PinLaterJobState.SUCCEEDED, jobInfo.getJobState());
    Assert.assertEquals(job.getNumAttemptsAllowed(), jobInfo.getAttemptsAllowed());
    Assert.assertEquals(job.getNumAttemptsAllowed(), jobInfo.getAttemptsRemaining());
    Assert.assertTrue(jobInfo.isSetCreatedAtTimestampMillis());
    Assert.assertTrue(jobInfo.isSetRunAfterTimestampMillis());
    // TODO: this should be changed to strictly > then when we migrate to MySQL 5.6 and are able to
    // store timestamps with precisions better than a second.
    Assert.assertTrue(jobInfo.getUpdatedAtTimestampMillis() >= oldUpdatedAt);
    Assert.assertTrue(jobInfo.getClaimDescriptor().contains("test"));
    Assert.assertTrue(Strings.isNullOrEmpty(jobInfo.getCustomStatus()));
    Assert.assertFalse(jobInfo.isSetBody());
  }

  @Test
  public void testPriorities() {
    int numJobs = 3;

    // Enqueue one of each priority job, starting from the lowest priority job (largest number).
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = numJobs; i > 0; i--) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(("job_body_" + i).getBytes()));
      job.setPriority((byte) i);
      enqueueRequest.addToJobs(job);
    }
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    List<String> jobDescriptors = enqueueResponse.getJobDescriptors();
    Assert.assertEquals(numJobs, enqueueResponse.getJobDescriptorsSize());
    Assert.assertEquals(numJobs, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));
    Assert.assertTrue(areJobsAvailable(getQueueName()));

    // Test lookupJob for each job.
    PinLaterLookupJobRequest lookupJobRequest = new PinLaterLookupJobRequest();
    lookupJobRequest.setIncludeBody(true);
    lookupJobRequest.setJobDescriptors(jobDescriptors);
    Map<String, PinLaterJobInfo> jobInfoMap = getBackend().lookupJobs(lookupJobRequest).get();
    // We should be able to find each original job descriptor in the returned map.
    for (String jobDesc : jobDescriptors) {
      PinLaterJobInfo jobInfo = jobInfoMap.get(jobDesc);
      Assert.assertNotNull(jobInfo);
      Assert.assertEquals(PinLaterJobState.PENDING, jobInfo.getJobState());
      jobInfoMap.remove(jobDesc);
    }
    Assert.assertTrue(jobInfoMap.isEmpty());

    // Dequeue jobs one at a time and make sure the higher priority ones come back in order.
    for (int priority = 1; priority <= numJobs; priority++) {
      String expectedJobDesc = jobDescriptors.get(numJobs - priority);
      PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
      PinLaterDequeueResponse
          dequeueResponse =
          getBackend().dequeueJobs("test", dequeueRequest).get();
      Assert.assertEquals(1, dequeueResponse.getJobsSize());
      Assert.assertTrue(dequeueResponse.getJobs().containsKey(expectedJobDesc));
      Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
          PinLaterJobState.IN_PROGRESS));

      // Ack the job.
      PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(getQueueName());
      ackRequest.addToJobsSucceeded(new PinLaterJobAckInfo(expectedJobDesc));
      getBackend().ackDequeuedJobs(ackRequest).get();
      Assert.assertEquals(priority, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
          PinLaterJobState.SUCCEEDED));
    }
    Assert.assertFalse(areJobsAvailable(getQueueName()));
  }

  @Test
  public void testRunAfter() throws InterruptedException {
    int delayMillis = 1000;

    // Enqueue a job with a runAfter timestamp in the future.
    long futureTimestampMillis = System.currentTimeMillis() + delayMillis;
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("job_body".getBytes()));
    job.setRunAfterTimestampMillis(futureTimestampMillis);
    enqueueRequest.addToJobs(job);
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(1, enqueueResponse.getJobDescriptorsSize());
    String jobDesc = enqueueResponse.getJobDescriptors().get(0);

    // There should be no jobs available right now.
    Assert.assertFalse(areJobsAvailable(getQueueName()));

    // Test lookupJob.
    PinLaterLookupJobRequest lookupJobRequest = new PinLaterLookupJobRequest();
    lookupJobRequest.setIncludeBody(true);
    lookupJobRequest.addToJobDescriptors(jobDesc);
    Map<String, PinLaterJobInfo> jobInfoMap = getBackend().lookupJobs(lookupJobRequest).get();
    Assert.assertEquals(1, jobInfoMap.size());
    Assert.assertEquals(PinLaterJobState.PENDING, jobInfoMap.get(jobDesc).getJobState());
    Assert.assertEquals(job.getNumAttemptsAllowed(),
        jobInfoMap.get(jobDesc).getAttemptsAllowed());
    Assert.assertEquals(job.getNumAttemptsAllowed(),
        jobInfoMap.get(jobDesc).getAttemptsRemaining());
    Assert.assertEquals("job_body", new String(jobInfoMap.get(jobDesc).getBody()));

    // Wait our original specified amount, and then there should be jobs available.
    Thread.sleep(delayMillis);
    Assert.assertTrue(areJobsAvailable(getQueueName()));

    // Dequeue returns the job.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 10);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(1, dequeueResponse.getJobsSize());
    Assert.assertTrue(dequeueResponse.getJobs().containsKey(jobDesc));
  }

  @Test
  public void testManyJobs() {
    // Enqueue 100 jobs with no retries and at random priorities.
    Random random = new Random();
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 100; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(new String("job_body_" + i).getBytes()));
      job.setNumAttemptsAllowed(1);
      job.setPriority((byte) (random.nextInt(3) + 1));
      enqueueRequest.addToJobs(job);
    }
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(100, enqueueResponse.getJobDescriptorsSize());
    Assert.assertEquals(100, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));
    Set<String> jobDescriptors = Sets.newHashSet(enqueueResponse.getJobDescriptors());

    // Now dequeue in batches of 10.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 10);
    for (int i = 0; i < 10; i++) {
      PinLaterDequeueResponse
          dequeueResponse =
          getBackend().dequeueJobs("test", dequeueRequest).get();
      for (String jobDesc : dequeueResponse.getJobs().keySet()) {
        jobDescriptors.remove(jobDesc);
      }
    }
    Assert.assertEquals(100, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    // Should have dequeued all 100 jobs.
    Assert.assertTrue(jobDescriptors.isEmpty());

    // Ack 50 jobs as succeeded, 50 as failed.
    PinLaterJobAckRequest jobAckRequest = new PinLaterJobAckRequest(getQueueName());
    boolean alternate = false;
    for (String jobDesc : enqueueResponse.getJobDescriptors()) {
      if (alternate) {
        jobAckRequest.addToJobsSucceeded(new PinLaterJobAckInfo(jobDesc));
      } else {
        jobAckRequest.addToJobsFailed(new PinLaterJobAckInfo(jobDesc));
      }
      alternate = !alternate;
    }
    getBackend().ackDequeuedJobs(jobAckRequest).get();
    Assert.assertEquals(50, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.SUCCEEDED));
    Assert.assertEquals(50, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.FAILED));
  }

  @Test
  public void testFailedRetry() {
    // Enqueue 5 jobs configured with 2 retries (3 attempts).
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 5; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(new String("job_body_" + i).getBytes()));
      job.setNumAttemptsAllowed(3);
      enqueueRequest.addToJobs(job);
    }
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(5, enqueueResponse.getJobDescriptorsSize());
    Assert.assertEquals(5, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));

    // Now dequeue all 5.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 5);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(5, dequeueResponse.getJobsSize());
    Assert.assertEquals(5, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    // ACK 2 of them as succeeded, 3 as failed.
    PinLaterJobAckRequest jobAckRequest = new PinLaterJobAckRequest(getQueueName());
    for (int i = 0; i < enqueueResponse.getJobDescriptorsSize(); i++) {
      PinLaterJobAckInfo jobAckInfo = new PinLaterJobAckInfo(
          enqueueResponse.getJobDescriptors().get(i));
      if (i < 2) {
        jobAckRequest.addToJobsSucceeded(jobAckInfo);
      } else {
        jobAckRequest.addToJobsFailed(jobAckInfo);
      }
    }
    getBackend().ackDequeuedJobs(jobAckRequest).get();
    Assert.assertEquals(2, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.SUCCEEDED));
    Assert.assertEquals(3, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));

    // Dequeue the remaining 3 jobs.
    dequeueResponse = getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(3, dequeueResponse.getJobsSize());
    Assert.assertEquals(3, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    // ACK 1 as succeeded, rest as failed.
    jobAckRequest.getJobsSucceeded().clear();
    jobAckRequest.getJobsFailed().clear();
    for (int i = 2; i < enqueueResponse.getJobDescriptorsSize(); i++) {
      PinLaterJobAckInfo jobAckInfo = new PinLaterJobAckInfo(
          enqueueResponse.getJobDescriptors().get(i));
      if (i == 2) {
        jobAckRequest.addToJobsSucceeded(jobAckInfo);
      } else {
        jobAckRequest.addToJobsFailed(jobAckInfo);
      }
    }
    getBackend().ackDequeuedJobs(jobAckRequest).get();
    Assert.assertEquals(3, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.SUCCEEDED));
    Assert.assertEquals(2, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));

    // Dequeue the remaining 2 jobs.
    dequeueResponse = getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(2, dequeueResponse.getJobsSize());
    Assert.assertEquals(2, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    // ACK both as failed. At this point, retries are exhausted, so they should go to FAILED.
    jobAckRequest.getJobsSucceeded().clear();
    jobAckRequest.getJobsFailed().clear();
    for (int i = 3; i < enqueueResponse.getJobDescriptorsSize(); i++) {
      PinLaterJobAckInfo jobAckInfo = new PinLaterJobAckInfo(
          enqueueResponse.getJobDescriptors().get(i));
      jobAckRequest.addToJobsFailed(jobAckInfo);
    }
    getBackend().ackDequeuedJobs(jobAckRequest).get();
    Assert.assertEquals(3, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.SUCCEEDED));
    Assert.assertEquals(2, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.FAILED));

    // Attempting to dequeue again should yield no more jobs.
    dequeueResponse = getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(0, dequeueResponse.getJobsSize());
  }

  @Test
  public void testAckDuringDequeue() {
    // Enqueue 5 jobs.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 5; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(new String("job_body_" + i).getBytes()));
      enqueueRequest.addToJobs(job);
    }
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(5, enqueueResponse.getJobDescriptorsSize());
    Assert.assertEquals(5, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));

    // Dequeue 2.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 2);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(2, dequeueResponse.getJobsSize());
    Assert.assertEquals(2, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    // Dequeue 2, ACK 2.
    PinLaterJobAckRequest jobAckRequest = new PinLaterJobAckRequest(getQueueName());
    for (String jobDesc : dequeueResponse.getJobs().keySet()) {
      jobAckRequest.addToJobsSucceeded(new PinLaterJobAckInfo(jobDesc));
    }
    dequeueRequest.setJobAckRequest(jobAckRequest);
    dequeueResponse = getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(2, dequeueResponse.getJobsSize());
    Assert.assertEquals(2, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));
    Assert.assertEquals(2, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.SUCCEEDED));

    // Dequeue 1, ACK 2.
    jobAckRequest.getJobsSucceeded().clear();
    for (String jobDesc : dequeueResponse.getJobs().keySet()) {
      jobAckRequest.addToJobsSucceeded(new PinLaterJobAckInfo(jobDesc));
    }
    dequeueResponse = getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(1, dequeueResponse.getJobsSize());
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));
    Assert.assertEquals(4, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.SUCCEEDED));

    // Dequeue 0, ACK 1.
    jobAckRequest.getJobsSucceeded().clear();
    for (String jobDesc : dequeueResponse.getJobs().keySet()) {
      jobAckRequest.addToJobsSucceeded(new PinLaterJobAckInfo(jobDesc));
    }
    dequeueResponse = getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(0, dequeueResponse.getJobsSize());
    Assert.assertEquals(0, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));
    Assert.assertEquals(5, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.SUCCEEDED));
  }

  @Test
  public void testAckFailedJob() {
    // Enqueue one job.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("job_body".getBytes()));
    enqueueRequest.addToJobs(job);
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(1, enqueueResponse.getJobDescriptorsSize());
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));
    String jobDesc = enqueueResponse.getJobDescriptors().get(0);

    // Check that jobs are available to dequeue.
    Assert.assertTrue(areJobsAvailable(getQueueName()));

    // Test lookupJob.
    PinLaterLookupJobRequest lookupJobRequest = new PinLaterLookupJobRequest();
    lookupJobRequest.setIncludeBody(true);
    lookupJobRequest.addToJobDescriptors(jobDesc);
    Map<String, PinLaterJobInfo> jobInfoMap = getBackend().lookupJobs(lookupJobRequest).get();
    Assert.assertEquals(1, jobInfoMap.size());
    Assert.assertEquals(PinLaterJobState.PENDING, jobInfoMap.get(jobDesc).getJobState());
    Assert.assertEquals(job.getNumAttemptsAllowed(),
        jobInfoMap.get(jobDesc).getAttemptsAllowed());
    Assert.assertEquals(job.getNumAttemptsAllowed(),
        jobInfoMap.get(jobDesc).getAttemptsRemaining());
    Assert.assertEquals("job_body", new String(jobInfoMap.get(jobDesc).getBody()));

    // Dequeue the job.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 10);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(1, dequeueResponse.getJobsSize());
    Assert.assertTrue(dequeueResponse.getJobs().containsKey(jobDesc));
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    // Ack the job failure first time.
    PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(getQueueName());
    ackRequest.addToJobsFailed(new PinLaterJobAckInfo(jobDesc));
    getBackend().ackDequeuedJobs(ackRequest).get();
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));

    // Test lookupJob.
    jobInfoMap = getBackend().lookupJobs(lookupJobRequest).get();
    Assert.assertEquals(1, jobInfoMap.size());
    Assert.assertEquals(PinLaterJobState.PENDING, jobInfoMap.get(jobDesc).getJobState());
    Assert.assertEquals(job.getNumAttemptsAllowed(),
        jobInfoMap.get(jobDesc).getAttemptsAllowed());
    Assert.assertEquals(job.getNumAttemptsAllowed() - 1,
        jobInfoMap.get(jobDesc).getAttemptsRemaining());
    Assert.assertEquals("job_body", new String(jobInfoMap.get(jobDesc).getBody()));

    // Dequeue the job second time.
    dequeueResponse = getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(1, dequeueResponse.getJobsSize());
    Assert.assertTrue(dequeueResponse.getJobs().containsKey(jobDesc));
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    // Ack the job failure second time.
    getBackend().ackDequeuedJobs(ackRequest).get();
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.FAILED));
  }

  @Test
  public void testAckJobNotDequeued() {
    // Enqueue one job.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("job_body".getBytes()));
    enqueueRequest.addToJobs(job);
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    Assert.assertEquals(1, enqueueResponse.getJobDescriptorsSize());
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));
    String jobDesc = enqueueResponse.getJobDescriptors().get(0);

    // Check that jobs are available to dequeue.
    Assert.assertTrue(areJobsAvailable(getQueueName()));

    // Test lookupJob.
    PinLaterLookupJobRequest lookupJobRequest = new PinLaterLookupJobRequest();
    lookupJobRequest.setIncludeBody(true);
    lookupJobRequest.addToJobDescriptors(jobDesc);
    Map<String, PinLaterJobInfo> jobInfoMap = getBackend().lookupJobs(lookupJobRequest).get();
    Assert.assertEquals(1, jobInfoMap.size());
    Assert.assertEquals(PinLaterJobState.PENDING, jobInfoMap.get(jobDesc).getJobState());
    Assert.assertEquals(job.getNumAttemptsAllowed(),
        jobInfoMap.get(jobDesc).getAttemptsAllowed());
    Assert.assertEquals(job.getNumAttemptsAllowed(),
        jobInfoMap.get(jobDesc).getAttemptsRemaining());
    Assert.assertEquals("job_body", new String(jobInfoMap.get(jobDesc).getBody()));

    // Ack the job which is still in pending queue will make no change.
    PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(getQueueName());
    ackRequest.addToJobsFailed(new PinLaterJobAckInfo(jobDesc));
    getBackend().ackDequeuedJobs(ackRequest).get();
    jobInfoMap = getBackend().lookupJobs(lookupJobRequest).get();
    Assert.assertEquals(1, jobInfoMap.size());
    Assert.assertEquals(PinLaterJobState.PENDING, jobInfoMap.get(jobDesc).getJobState());
    Assert.assertEquals("job_body", new String(jobInfoMap.get(jobDesc).getBody()));
  }

  @Test
  public void testRetryDelay() throws InterruptedException {
    int retryDelayMillis = 2000;

    // Enqueue a job.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("job_body".getBytes()));
    enqueueRequest.addToJobs(job);
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    String jobDesc = enqueueResponse.getJobDescriptors().get(0);

    // Dequeue the job.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(1, dequeueResponse.getJobsSize());
    Assert.assertTrue(dequeueResponse.getJobs().containsKey(jobDesc));
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    // Ack the job with a retry delay.
    PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(getQueueName());
    PinLaterJobAckInfo ackInfo = new PinLaterJobAckInfo(jobDesc);
    ackInfo.setRetryDelayMillis(retryDelayMillis);
    ackRequest.addToJobsFailed(ackInfo);
    getBackend().ackDequeuedJobs(ackRequest).get();

    // Check to see that we can't dequeue it right now, but can later.
    dequeueResponse = getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(0, dequeueResponse.getJobsSize());
    Thread.sleep(retryDelayMillis);
    dequeueResponse = getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(1, dequeueResponse.getJobsSize());
    Assert.assertTrue(dequeueResponse.getJobs().containsKey(jobDesc));
  }

  @Test
  public void testDryRunGetJobBody() throws InterruptedException {
    // Enqueue a job.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    ByteBuffer jobBody = ByteBuffer.wrap("job_body".getBytes());
    PinLaterJob job = new PinLaterJob(jobBody);
    enqueueRequest.addToJobs(job);
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    String jobDesc = enqueueResponse.getJobDescriptors().get(0);
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));

    // Test retrieving the job.
    PinLaterDequeueRequest oneDryRunJobRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    oneDryRunJobRequest.setDryRun(true);
    Map<String, ByteBuffer>
        jobs =
        getBackend().dequeueJobs("test", oneDryRunJobRequest).get().getJobs();
    Assert.assertEquals(jobBody, jobs.get(jobDesc));
    Assert.assertEquals(1, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));
  }

  @Test
  public void testGetJobCount() {
    // Enqueue 20 P1 jobs to run now and check getJobCount results.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 20; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(new String("job_body_" + i).getBytes()));
      enqueueRequest.addToJobs(job);
    }
    getBackend().enqueueJobs(enqueueRequest).get();
    PinLaterGetJobCountRequest getJobCountRequest = new PinLaterGetJobCountRequest();
    getJobCountRequest.setQueueName(getQueueName());
    getJobCountRequest.setPriority((byte) 1);
    getJobCountRequest.setJobState(PinLaterJobState.PENDING);
    Assert.assertEquals(20, (int) getBackend().getJobCount(getJobCountRequest).get());

    // Also verify that getJobCount works if we specify that the request should count only jobs
    // with bodies matching a regex.
    getJobCountRequest.setBodyRegexToMatch("job_body_1.*");
    Assert.assertEquals(11, (int) getBackend().getJobCount(getJobCountRequest).get());
    getJobCountRequest.setBodyRegexToMatch("job_body_*");
    Assert.assertEquals(20, (int) getBackend().getJobCount(getJobCountRequest).get());

    // Enqueue 15 P2 jobs to run later and check getJobCount results.
    enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 15; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(new String("job_body_" + i).getBytes()));
      job.setPriority((byte) 2);
      job.setRunAfterTimestampMillis(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(100));
      enqueueRequest.addToJobs(job);
    }
    getBackend().enqueueJobs(enqueueRequest).get();
    getJobCountRequest = new PinLaterGetJobCountRequest();
    getJobCountRequest.setQueueName(getQueueName());
    getJobCountRequest.setPriority((byte) 2);
    getJobCountRequest.setJobState(PinLaterJobState.PENDING);
    getJobCountRequest.setCountFutureJobs(false);
    Assert.assertEquals(0, (int) getBackend().getJobCount(getJobCountRequest).get());
    getJobCountRequest.setCountFutureJobs(true);
    Assert.assertEquals(15, (int) getBackend().getJobCount(getJobCountRequest).get());

    // Dequeue 5 of the P1 jobs to test counting job states.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 5);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    getJobCountRequest = new PinLaterGetJobCountRequest();
    getJobCountRequest.setQueueName(getQueueName());
    getJobCountRequest.setPriority((byte) 1);
    getJobCountRequest.setJobState(PinLaterJobState.IN_PROGRESS);
    Assert.assertEquals(5, (int) getBackend().getJobCount(getJobCountRequest).get());

    // Ack the 5 jobs and count the completed jobs.
    PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(getQueueName());
    for (String jobDesc : dequeueResponse.getJobs().keySet()) {
      ackRequest.addToJobsSucceeded(new PinLaterJobAckInfo(jobDesc));
    }
    getBackend().ackDequeuedJobs(ackRequest).get();
    getJobCountRequest = new PinLaterGetJobCountRequest();
    getJobCountRequest.setQueueName(getQueueName());
    getJobCountRequest.setPriority((byte) 1);
    getJobCountRequest.setJobState(PinLaterJobState.SUCCEEDED);
    Assert.assertEquals(5, (int) getBackend().getJobCount(getJobCountRequest).get());

    // Add 10 current P2 jobs and check that we can simultaneously count pending P1 and P2 jobs.
    enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 10; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(new String("job_body_" + i).getBytes()));
      job.setPriority((byte) 2);
      enqueueRequest.addToJobs(job);
    }
    getBackend().enqueueJobs(enqueueRequest).get();
    getJobCountRequest = new PinLaterGetJobCountRequest();
    getJobCountRequest.setQueueName(getQueueName());
    getJobCountRequest.setJobState(PinLaterJobState.PENDING);
    Assert.assertEquals(25, (int) getBackend().getJobCount(getJobCountRequest).get());
  }

  @Test
  public void testCreateQueueThatAlreadyExists() {
    // Test queue should already exist before this call. This call should not throw any errors
    // since createQueue() is idempotent and acts as a no-op if the queue already exists.
    getBackend().createQueue(getQueueName()).get();
  }

  @Test
  public void testDeleteQueueThatDoesNotExist() {
    // Test queue should already exist before this call, so we call delete twice to test that it
    // is in fact idempotent and acts as a no-op if the queue doesn't exist.
    getBackend().deleteQueue(getQueueName()).get();
    getBackend().deleteQueue(getQueueName()).get();
  }

  @Test
  public void testDeleteQueuePassword() {
    try {
      getBackend().deleteQueue(getQueueName(), "bad_password").get();
    } catch (Exception e) {
      if (e instanceof PinLaterException) {
        Assert.assertEquals(ErrorCode.PASSWORD_INVALID, ((PinLaterException) e).getErrorCode());
        return;
      }
    }
    Assert.fail();
  }

  @Test
  public void testScanJobsBadContinuation() {
    try {
      PinLaterScanJobsRequest scanJobsRequest = new PinLaterScanJobsRequest();
      scanJobsRequest.setQueueName(getQueueName());
      scanJobsRequest.setContinuation("INVALID_CONTINUATION");
      getBackend().scanJobs(scanJobsRequest).get();
    } catch (Exception e) {
      if (e instanceof PinLaterException) {
        Assert.assertEquals(ErrorCode.CONTINUATION_INVALID, ((PinLaterException) e).getErrorCode());
        return;
      }
    }
    Assert.fail();
  }

  @Test
  public void testDequeueMetadata() throws InterruptedException {
    // Enqueue one job.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("job_body".getBytes()));
    job.setNumAttemptsAllowed(2);
    enqueueRequest.addToJobs(job);
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
    String jobDesc = enqueueResponse.getJobDescriptors().get(0);

    // Dequeue the job and ack as failed.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    getBackend().dequeueJobs(getQueueName(), dequeueRequest).get();
    PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(getQueueName());
    ackRequest.addToJobsFailed(new PinLaterJobAckInfo(jobDesc));
    getBackend().ackDequeuedJobs(ackRequest).get();

    // Dequeue the job with dryRun on and check the dequeue metadata returned (one of the attempts
    // allowed should be used up).
    dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    dequeueRequest.setDryRun(true);
    PinLaterDequeueResponse dequeueResponse =
        getBackend().dequeueJobs(getQueueName(), dequeueRequest).get();
    PinLaterDequeueMetadata metadata = dequeueResponse.getJobMetadata().get(jobDesc);
    Assert.assertEquals(2, metadata.getAttemptsAllowed());
    Assert.assertEquals(1, metadata.getAttemptsRemaining());

    // Dequeue the job and check the dequeue metadata returned (one of the attempts allowed
    // should be used up).
    dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    dequeueResponse = getBackend().dequeueJobs(getQueueName(), dequeueRequest).get();
    metadata = dequeueResponse.getJobMetadata().get(jobDesc);
    Assert.assertEquals(2, metadata.getAttemptsAllowed());
    Assert.assertEquals(1, metadata.getAttemptsRemaining());
    ackRequest = new PinLaterJobAckRequest(getQueueName());
    ackRequest.addToJobsFailed(new PinLaterJobAckInfo(jobDesc));
    getBackend().ackDequeuedJobs(ackRequest).get();
  }

  @Test
  public void testDeleteQueue() {
    // Note that queue should exist from beforeTest().
    Assert.assertTrue(getBackend().getQueueNames().get().contains(getQueueName()));
    getBackend().deleteQueue(getQueueName()).get();
    Assert.assertFalse(getBackend().getQueueNames().get().contains(getQueueName()));
  }

  private boolean areJobsAvailable(String queueName) {
    PinLaterDequeueRequest getOneJobRequest = new PinLaterDequeueRequest(queueName, 1);
    getOneJobRequest.setDryRun(true);
    Map<String, ByteBuffer>
        jobs =
        getBackend().dequeueJobs("test", getOneJobRequest).get().getJobs();
    return jobs != null && jobs.size() > 0;
  }

  @Test
  public void testScanJobs() throws InterruptedException {
    // Enqueue 20 jobs (10 P1 and 10 P2) that are to be run right now. These jobs are to have
    // bodies "job_body_1" to "job_body_20".
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    long currentTimeMillis = System.currentTimeMillis();
    for (int i = 1; i <= 20; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(("job_body_" + i).getBytes()));
      job.setRunAfterTimestampMillis(currentTimeMillis - 10000 * i);
      if (i % 2 == 0) {
        job.setPriority((byte) 1);
      } else {
        job.setPriority((byte) 2);
      }
      enqueueRequest.setQueueName(getQueueName());
      enqueueRequest.addToJobs(job);
    }
    getBackend().enqueueJobs(enqueueRequest).get();

    // Enqueue 30 jobs (15 P1 and 15 P2) that are to be run in the future. These jobs are to have
    // bodies "job_body_21" to "job_body_50".
    enqueueRequest = new PinLaterEnqueueRequest();
    currentTimeMillis = System.currentTimeMillis();
    for (int i = 21; i <= 50; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(("job_body_" + i).getBytes()));
      job.setRunAfterTimestampMillis(currentTimeMillis + 10000 * i);
      if (i % 2 == 0) {
        job.setPriority((byte) 1);
      } else {
        job.setPriority((byte) 2);
      }
      enqueueRequest.setQueueName(getQueueName());
      enqueueRequest.addToJobs(job);
    }
    getBackend().enqueueJobs(enqueueRequest).get();

    // Scan for P1 jobs that are able to be run now: there should only be 10 of them. Check that
    // the 10 jobs returned are able to run now and ordered by run_after descending.
    PinLaterScanJobsRequest scanRequest = new PinLaterScanJobsRequest();
    scanRequest.setQueueName(getQueueName());
    scanRequest.setJobState(PinLaterJobState.PENDING);
    scanRequest.setPriority((byte) 1);
    PinLaterScanJobsResponse scanJobsResponse = getBackend().scanJobs(scanRequest).get();
    List<PinLaterJobInfo> jobs = scanJobsResponse.getScannedJobs();
    Assert.assertEquals(10, jobs.size());
    PinLaterJobInfo lastJobInfo = jobs.get(0);
    for (int i = 0; i < jobs.size(); i++) {
      PinLaterJobInfo currentJobInfo = jobs.get(i);

      // Assert all the desired fields are set (except body, which is set only if explicitly
      // requested).
      Assert.assertFalse(Strings.isNullOrEmpty(currentJobInfo.getJobDescriptor()));
      Assert.assertEquals(PinLaterJobState.PENDING, currentJobInfo.getJobState());
      Assert.assertEquals(2, currentJobInfo.getAttemptsAllowed());
      Assert.assertEquals(2, currentJobInfo.getAttemptsRemaining());
      Assert.assertTrue(currentJobInfo.isSetCreatedAtTimestampMillis());
      Assert.assertTrue(currentJobInfo.isSetUpdatedAtTimestampMillis());
      Assert.assertTrue(currentJobInfo.getRunAfterTimestampMillis() < currentTimeMillis);
      Assert.assertTrue(Strings.isNullOrEmpty(currentJobInfo.getClaimDescriptor()));
      Assert.assertTrue(Strings.isNullOrEmpty(currentJobInfo.getCustomStatus()));
      Assert.assertFalse(currentJobInfo.isSetBody());

      Assert.assertTrue(
          currentJobInfo.getRunAfterTimestampMillis() <= lastJobInfo.getRunAfterTimestampMillis());
      lastJobInfo = currentJobInfo;
    }

    // Scan for P1 jobs again, but this time search for jobs with bodies matching a specified
    // regex. Check that the 5 jobs returned are able to run now and ordered by run_after
    // descending.
    scanRequest.setBodyRegexToMatch("job_body_1.*");
    jobs = getBackend().scanJobs(scanRequest).get().getScannedJobs();
    Assert.assertEquals(5, jobs.size());
    lastJobInfo = jobs.get(0);
    for (int i = 0; i < jobs.size(); i++) {
      PinLaterJobInfo currentJobInfo = jobs.get(i);

      // Assert all the desired fields are set (except body, which is set only if explicitly
      // requested).
      Assert.assertFalse(Strings.isNullOrEmpty(currentJobInfo.getJobDescriptor()));
      Assert.assertEquals(PinLaterJobState.PENDING, currentJobInfo.getJobState());
      Assert.assertEquals(2, currentJobInfo.getAttemptsAllowed());
      Assert.assertEquals(2, currentJobInfo.getAttemptsRemaining());
      Assert.assertTrue(currentJobInfo.isSetCreatedAtTimestampMillis());
      Assert.assertTrue(currentJobInfo.isSetUpdatedAtTimestampMillis());
      Assert.assertTrue(currentJobInfo.getRunAfterTimestampMillis() < currentTimeMillis);
      Assert.assertTrue(Strings.isNullOrEmpty(currentJobInfo.getClaimDescriptor()));
      Assert.assertTrue(Strings.isNullOrEmpty(currentJobInfo.getCustomStatus()));
      Assert.assertFalse(currentJobInfo.isSetBody());

      Assert.assertTrue(
          currentJobInfo.getRunAfterTimestampMillis() <= lastJobInfo.getRunAfterTimestampMillis());
      lastJobInfo = currentJobInfo;
    }

    // Scan for 20 jobs (of all priorities) that are scheduled to be run in the future.
    // Check that the 20 jobs returned are able to run now and ordered by run_after descending.
    scanRequest = new PinLaterScanJobsRequest();
    scanRequest.setQueueName(getQueueName());
    scanRequest.setJobState(PinLaterJobState.PENDING);
    scanRequest.setScanFutureJobs(true);
    scanRequest.setLimit(20);
    scanJobsResponse = getBackend().scanJobs(scanRequest).get();
    jobs = scanJobsResponse.getScannedJobs();
    Assert.assertEquals(20, jobs.size());
    lastJobInfo = jobs.get(0);
    for (int i = 1; i < jobs.size(); i++) {
      PinLaterJobInfo currentJobInfo = jobs.get(i);

      // Assert all the desired fields are set (except body, which is set only if explicitly
      // requested).
      Assert.assertFalse(Strings.isNullOrEmpty(currentJobInfo.getJobDescriptor()));
      Assert.assertEquals(PinLaterJobState.PENDING, currentJobInfo.getJobState());
      Assert.assertEquals(2, currentJobInfo.getAttemptsAllowed());
      Assert.assertEquals(2, currentJobInfo.getAttemptsRemaining());
      Assert.assertTrue(currentJobInfo.isSetCreatedAtTimestampMillis());
      Assert.assertTrue(currentJobInfo.isSetUpdatedAtTimestampMillis());
      Assert.assertTrue(currentJobInfo.getRunAfterTimestampMillis() > currentTimeMillis);
      Assert.assertTrue(Strings.isNullOrEmpty(currentJobInfo.getClaimDescriptor()));
      Assert.assertTrue(Strings.isNullOrEmpty(currentJobInfo.getCustomStatus()));
      Assert.assertFalse(currentJobInfo.isSetBody());

      Assert.assertTrue(
          currentJobInfo.getRunAfterTimestampMillis() <= lastJobInfo.getRunAfterTimestampMillis());
      lastJobInfo = currentJobInfo;
    }

    // Again scan for 5 jobs of all priorities scheduled to be run in the future, but this time
    // search for jobs with bodies matching a specified regex. Check that the 5 jobs returned are
    // not scheduled to run yet and ordered by run_after descending.
    scanRequest.setLimit(5);
    scanRequest.setBodyRegexToMatch("job_body_3.*");
    jobs = getBackend().scanJobs(scanRequest).get().getScannedJobs();
    Assert.assertEquals(5, jobs.size());
    lastJobInfo = jobs.get(0);
    for (int i = 1; i < jobs.size(); i++) {
      PinLaterJobInfo currentJobInfo = jobs.get(i);

      // Assert all the desired fields are set (except body, which is set only if explicitly
      // requested).
      Assert.assertFalse(Strings.isNullOrEmpty(currentJobInfo.getJobDescriptor()));
      Assert.assertEquals(PinLaterJobState.PENDING, currentJobInfo.getJobState());
      Assert.assertEquals(2, currentJobInfo.getAttemptsAllowed());
      Assert.assertEquals(2, currentJobInfo.getAttemptsRemaining());
      Assert.assertTrue(currentJobInfo.isSetCreatedAtTimestampMillis());
      Assert.assertTrue(currentJobInfo.isSetUpdatedAtTimestampMillis());
      Assert.assertTrue(currentJobInfo.getRunAfterTimestampMillis() > currentTimeMillis);
      Assert.assertTrue(Strings.isNullOrEmpty(currentJobInfo.getClaimDescriptor()));
      Assert.assertTrue(Strings.isNullOrEmpty(currentJobInfo.getCustomStatus()));
      Assert.assertFalse(currentJobInfo.isSetBody());
      Assert.assertTrue(currentJobInfo.getRunAfterTimestampMillis() > currentTimeMillis);

      Assert.assertTrue(
          currentJobInfo.getRunAfterTimestampMillis() <= lastJobInfo.getRunAfterTimestampMillis());
      lastJobInfo = currentJobInfo;
    }
  }

  @Test
  public void testGetQueueNames() {
    // There should be 1 test queue already created from the beforeTest method.
    // Note: the dev environment could already have other queues created besides this test queue.
    Assert.assertTrue(getBackend().getQueueNames().get().contains(getQueueName()));

    // Remove the test queue, create 3 more queues.
    getBackend().deleteQueue(getQueueName()).get();
    Set<String> testQueueNames = Sets.newHashSet();
    for (int i = 0; i < 3; i++) {
      String queueName = getQueueName() + i;
      testQueueNames.add(queueName);
      // Delete the queue first in case it was there previously for some reason.
      getBackend().deleteQueue(queueName).get();
      getBackend().createQueue(queueName).get();
    }
    Set<String> queueNames = getBackend().getQueueNames().get();
    Assert.assertFalse(queueNames.contains(getQueueName()));
    Assert.assertTrue(queueNames.containsAll(testQueueNames));

    // Clean up the queues we created.
    for (String queueName : testQueueNames) {
      getBackend().deleteQueue(queueName).get();
    }
  }

  @Test
  public void testQueueDoesNotExistEnqueue() {
    try {
      PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
      enqueueRequest.setQueueName("nonexistent_queue");
      enqueueRequest.addToJobs(new PinLaterJob(ByteBuffer.wrap("job_body".getBytes())));
      getBackend().enqueueJobs(enqueueRequest).get();
    } catch (Exception e) {
      if (e instanceof PinLaterException) {
        Assert.assertEquals(ErrorCode.QUEUE_NOT_FOUND, ((PinLaterException) e).getErrorCode());
        return;
      }
    }
    Assert.fail();
  }

  @Test
  public void testPriorityNotSupportedEnqueue() {
    try {
      PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
      enqueueRequest.setQueueName(getQueueName());
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("job_body".getBytes()));
      job.setPriority((byte) 4);
      enqueueRequest.addToJobs(job);
      getBackend().enqueueJobs(enqueueRequest).get();
    } catch (Exception e) {
      if (e instanceof PinLaterException) {
        Assert.assertEquals(ErrorCode.PRIORITY_NOT_SUPPORTED,
            ((PinLaterException) e).getErrorCode());
        return;
      }
    }
    Assert.fail();
  }

  @Test
  public void testRetryFailedJobs() throws InterruptedException {
    int retryDelayMillis = 1000;

    // Enqueue 20 P2 jobs with 1 attempt allowed each.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 20; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(new String("job_body_" + i).getBytes()));
      job.setNumAttemptsAllowed(1);
      job.setPriority((byte) 2);
      enqueueRequest.addToJobs(job);
    }
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();

    // Dequeue all 20 jobs and ack all of them as failures.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 20);
    getBackend().dequeueJobs("test", dequeueRequest).get();
    PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(getQueueName());
    List<String> jobDescriptors = enqueueResponse.getJobDescriptors();
    for (String jobDesc : jobDescriptors) {
      ackRequest.addToJobsFailed(new PinLaterJobAckInfo(jobDesc));
    }
    getBackend().ackDequeuedJobs(ackRequest).get();
    Assert.assertEquals(0, getBackend().dequeueJobs("test", dequeueRequest).get().getJobsSize());

    // Try to retry P1 jobs; nothing should happen since we only enqueued P2 jobs.
    PinLaterRetryFailedJobsRequest failedJobsRequest =
        new PinLaterRetryFailedJobsRequest(getQueueName());
    Assert.assertEquals(0, getBackend().retryFailedJobs(failedJobsRequest).get().intValue());
    Assert.assertEquals(0, getBackend().dequeueJobs("test", dequeueRequest).get().getJobsSize());

    // Try to retry 15 P2 jobs by granting 2 attempts to each job, and set the run after timestamp
    // to have some delay from now.
    failedJobsRequest = new PinLaterRetryFailedJobsRequest(getQueueName());
    failedJobsRequest.setAttemptsToAllow(2);
    failedJobsRequest.setPriority((byte) 2);
    failedJobsRequest.setLimit(15);
    failedJobsRequest.setRunAfterTimestampMillis(System.currentTimeMillis() + retryDelayMillis);
    Assert.assertEquals(15, getBackend().retryFailedJobs(failedJobsRequest).get().intValue());

    // Assert that during this delay there are no dequeues allowed. Then wait for the delay to
    // pass.
    Assert.assertEquals(0, getBackend().dequeueJobs("test", dequeueRequest).get().getJobsSize());
    Thread.sleep(retryDelayMillis);

    // Dequeue those 15 jobs that are now available and ack them as failures. Do this twice so we
    // can get rid of both of their granted retries. There should be no jobs left pending at the
    // end.
    for (int i = 0; i < 2; i++) {
      dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 20);
      PinLaterDequeueResponse
          dequeueResponse =
          getBackend().dequeueJobs("test", dequeueRequest).get();
      Assert.assertEquals(15, dequeueResponse.getJobsSize());

      ackRequest = new PinLaterJobAckRequest(getQueueName());
      jobDescriptors = enqueueResponse.getJobDescriptors();
      for (String jobDesc : jobDescriptors) {
        ackRequest.addToJobsFailed(new PinLaterJobAckInfo(jobDesc));
      }
      getBackend().ackDequeuedJobs(ackRequest).get();
    }
    Assert.assertEquals(0, getBackend().dequeueJobs("test", dequeueRequest).get().getJobsSize());
  }

  @Test
  public void testDeleteJobs() {
    // Enqueue 10 P2 jobs with 1 attempt allowed each.
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 10; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(new String("failed_" + i).getBytes()));
      job.setNumAttemptsAllowed(1);
      job.setPriority((byte) 2);
      enqueueRequest.addToJobs(job);
    }
    PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();

    // Dequeue 10 jobs and ack them as failures.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 10);
    getBackend().dequeueJobs("test", dequeueRequest).get();
    PinLaterJobAckRequest ackRequest = new PinLaterJobAckRequest(getQueueName());
    List<String> jobDescriptors = enqueueResponse.getJobDescriptors();
    for (String jobDesc : jobDescriptors) {
      ackRequest.addToJobsFailed(new PinLaterJobAckInfo(jobDesc));
    }
    getBackend().ackDequeuedJobs(ackRequest).get();

    // Try to delete 1 P1 job, but only P2 jobs were enqueued so this shouldn't do anything.
    PinLaterDeleteJobsRequest deleteJobsRequest =
        new PinLaterDeleteJobsRequest(getQueueName(), PinLaterJobState.PENDING);
    Assert.assertEquals(0, getBackend().deleteJobs(deleteJobsRequest).get().intValue());

    // Delete 5 failed P2 jobs. Check there are 5 failed P2 jobs remaining.
    deleteJobsRequest = new PinLaterDeleteJobsRequest(getQueueName(), PinLaterJobState.FAILED);
    deleteJobsRequest.setPriority((byte) 2);
    deleteJobsRequest.setLimit((byte) 5);
    Assert.assertEquals(5, getBackend().deleteJobs(deleteJobsRequest).get().intValue());
    PinLaterGetJobCountRequest getJobCountRequest =
        new PinLaterGetJobCountRequest(getQueueName(), PinLaterJobState.FAILED);
    getJobCountRequest.setPriority((byte) 2);
    Assert.assertEquals(5, getBackend().getJobCount(getJobCountRequest).get().intValue());

    // Enqueue 10 more P2 jobs (these will be in the PENDING state).
    enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    for (int i = 0; i < 10; i++) {
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(new String("pending_" + (i)).getBytes()));
      job.setPriority((byte) 2);
      enqueueRequest.addToJobs(job);
    }
    enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();

    // Delete 1 failed P2 job containing the body "pending_1" and verify that it was actually
    // deleted.
    deleteJobsRequest = new PinLaterDeleteJobsRequest(getQueueName(), PinLaterJobState.PENDING);
    deleteJobsRequest.setPriority((byte) 2);
    deleteJobsRequest.setBodyRegexToMatch(".*pending_1.*");
    Assert.assertEquals(1, getBackend().deleteJobs(deleteJobsRequest).get().intValue());
    String deletedJobDescriptor = enqueueResponse.getJobDescriptors().get(1);
    PinLaterLookupJobRequest lookupJobRequest =
        new PinLaterLookupJobRequest(Arrays.asList(deletedJobDescriptor));
    Assert.assertFalse(
        getBackend().lookupJobs(lookupJobRequest).get().containsKey(deletedJobDescriptor));
    getJobCountRequest = new PinLaterGetJobCountRequest(getQueueName(), PinLaterJobState.PENDING);
    getJobCountRequest.setPriority((byte) 2);
    Assert.assertEquals(9, getBackend().getJobCount(getJobCountRequest).get().intValue());
  }

  @Test
  public void testCheckpointJob() throws InterruptedException {
    // Enqueue a job.
    long runAfterMillis = System.currentTimeMillis();
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("old_body".getBytes()));
    job.setRunAfterTimestampMillis(runAfterMillis);
    enqueueRequest.addToJobs(job);
    String jobDesc = getBackend().enqueueJobs(enqueueRequest).get().getJobDescriptors().get(0);

    // Dequeue the job so it's in a running state.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    getBackend().dequeueJobs("test-dequeuer", dequeueRequest).get();

    // Wait 1 second, then try to checkpoint one of the jobs with default params. The 1 second
    // wait is necessary because MySQL timestamps have a resolution of a second.
    Thread.sleep(1000);
    PinLaterCheckpointJobRequest checkpointJobRequest =
        new PinLaterCheckpointJobRequest(jobDesc);
    PinLaterCheckpointJobsRequest checkpointJobsRequest = new PinLaterCheckpointJobsRequest(
        getQueueName(), Lists.newArrayList(checkpointJobRequest));
    getBackend().checkpointJobs("test-dequeuer", checkpointJobsRequest).get();

    // Get the current run_after timestamp and then verify that checkpointing updated the
    // run_after timestamp to something in the future and kept all other fields the same.
    PinLaterLookupJobRequest lookupJobRequest = new PinLaterLookupJobRequest(
        Lists.newArrayList(jobDesc));
    lookupJobRequest.setIncludeBody(true);
    PinLaterJobInfo jobInfo = getBackend().lookupJobs(lookupJobRequest).get().get(jobDesc);
    Assert.assertTrue(jobInfo.getRunAfterTimestampMillis() > runAfterMillis);
    Assert.assertEquals(PinLaterJobState.IN_PROGRESS, jobInfo.getJobState());
    Assert.assertEquals("old_body", new String(jobInfo.getBody()));
    Assert.assertTrue(jobInfo.getClaimDescriptor().contains("test"));
  }

  @Test
  public void testCheckpointJobNewBody() {
    // Enqueue a job.
    long runAfterMillis = System.currentTimeMillis();
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("old_body".getBytes()));
    job.setRunAfterTimestampMillis(runAfterMillis);
    enqueueRequest.addToJobs(job);
    String jobDesc = getBackend().enqueueJobs(enqueueRequest).get().getJobDescriptors().get(0);

    // Dequeue the job so it's in a running state.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    getBackend().dequeueJobs("test-dequeuer", dequeueRequest).get();

    // Try to checkpoint one of the jobs with a new body.
    PinLaterCheckpointJobRequest checkpointJobRequest = new PinLaterCheckpointJobRequest(jobDesc);
    checkpointJobRequest.setNewBody("new_body");
    PinLaterCheckpointJobsRequest checkpointJobsRequest = new PinLaterCheckpointJobsRequest(
        getQueueName(), Lists.newArrayList(checkpointJobRequest));
    getBackend().checkpointJobs("test-dequeuer", checkpointJobsRequest).get();

    // Get the current run_after timestamp and then verify that checkpointing updated the
    // body but kept the other fields constant.
    PinLaterLookupJobRequest lookupJobRequest =
        new PinLaterLookupJobRequest(Lists.newArrayList(jobDesc));
    lookupJobRequest.setIncludeBody(true);
    PinLaterJobInfo jobInfo = getBackend().lookupJobs(lookupJobRequest).get().get(jobDesc);
    Assert.assertEquals(PinLaterJobState.IN_PROGRESS, jobInfo.getJobState());
    Assert.assertEquals("new_body", new String(jobInfo.getBody()));
    Assert.assertTrue(jobInfo.getClaimDescriptor().contains("test"));
  }

  @Test
  public void testCheckpointJobMoveToPending() {
    // Enqueue a job.
    long runAfterMillis = System.currentTimeMillis();
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("old_body".getBytes()));
    job.setRunAfterTimestampMillis(runAfterMillis);
    enqueueRequest.addToJobs(job);
    String jobDesc = getBackend().enqueueJobs(enqueueRequest).get().getJobDescriptors().get(0);

    // Dequeue the job so it's in a running state.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    getBackend().dequeueJobs("test-dequeuer", dequeueRequest).get();

    // Try to checkpoint one of the jobs with moveToPending = true.
    PinLaterCheckpointJobRequest checkpointJobRequest = new PinLaterCheckpointJobRequest(jobDesc);
    checkpointJobRequest.setMoveToPending(true);
    PinLaterCheckpointJobsRequest checkpointJobsRequest = new PinLaterCheckpointJobsRequest(
        getQueueName(), Lists.newArrayList(checkpointJobRequest));
    getBackend().checkpointJobs("test-dequeuer", checkpointJobsRequest).get();

    // Get the current run_after timestamp and then verify that checkpointing updated the
    // job's state, nullify the claim description but kept all other fields the same.
    PinLaterLookupJobRequest lookupJobRequest =
        new PinLaterLookupJobRequest(Lists.newArrayList(jobDesc));
    lookupJobRequest.setIncludeBody(true);
    PinLaterJobInfo jobInfo = getBackend().lookupJobs(lookupJobRequest).get().get(jobDesc);
    Assert.assertEquals(PinLaterJobState.PENDING, jobInfo.getJobState());
    Assert.assertEquals("old_body", new String(jobInfo.getBody()));
    Assert.assertNull(jobInfo.getClaimDescriptor());
  }

  @Test
  public void testCheckpointJobNewRunAfter() {
    // Enqueue a job.
    long runAfterMillis = System.currentTimeMillis();
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("old_body".getBytes()));
    job.setRunAfterTimestampMillis(runAfterMillis);
    enqueueRequest.addToJobs(job);
    String jobDesc = getBackend().enqueueJobs(enqueueRequest).get().getJobDescriptors().get(0);

    // Dequeue the job so it's in a running state.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    getBackend().dequeueJobs("test-dequeuer", dequeueRequest).get();

    // Try to checkpoint the job and set a new run_after timestamp.
    PinLaterCheckpointJobRequest checkpointJobRequest = new PinLaterCheckpointJobRequest(jobDesc);
    long newRunAfterMillis = runAfterMillis + TimeUnit.SECONDS.toMillis(10);
    checkpointJobRequest.setRunAfterTimestampMillis(newRunAfterMillis);
    PinLaterCheckpointJobsRequest checkpointJobsRequest = new PinLaterCheckpointJobsRequest(
        getQueueName(), Lists.newArrayList(checkpointJobRequest));
    getBackend().checkpointJobs("test-dequeuer", checkpointJobsRequest).get();

    // Get the current run_after timestamp and then verify that checkpointing updated the
    // job's run_after timestamp but kept all other fields the same.
    PinLaterLookupJobRequest lookupJobRequest =
        new PinLaterLookupJobRequest(Lists.newArrayList(jobDesc));
    lookupJobRequest.setIncludeBody(true);
    PinLaterJobInfo jobInfo = getBackend().lookupJobs(lookupJobRequest).get().get(jobDesc);
    // Note: MySQL only has a resolution of up to a second and does not round (it truncates the
    // millisecond), so we compare the expected and actual values in seconds.
    Assert.assertEquals(TimeUnit.MILLISECONDS.toSeconds(newRunAfterMillis),
        TimeUnit.MILLISECONDS.toSeconds(jobInfo.getRunAfterTimestampMillis()));
    Assert.assertEquals(PinLaterJobState.IN_PROGRESS, jobInfo.getJobState());
    Assert.assertEquals("old_body", new String(jobInfo.getBody()));
  }

  @Test
  public void testCheckpointJobNoOp() {
    // Enqueue a job.
    long runAfterMillis = System.currentTimeMillis();
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("old_body".getBytes()));
    job.setRunAfterTimestampMillis(runAfterMillis);
    enqueueRequest.addToJobs(job);
    String jobDesc = getBackend().enqueueJobs(enqueueRequest).get().getJobDescriptors().get(0);

    // Try to checkpoint the job (which is still pending). This operation should be a no-op.
    PinLaterCheckpointJobRequest checkpointJobRequest = new PinLaterCheckpointJobRequest(jobDesc);
    PinLaterCheckpointJobsRequest checkpointJobsRequest = new PinLaterCheckpointJobsRequest(
        getQueueName(), Lists.newArrayList(checkpointJobRequest));
    getBackend().checkpointJobs("test-dequeuer", checkpointJobsRequest).get();
    // Verify that the job is still pending and not changed.
    PinLaterLookupJobRequest lookupJobRequest =
        new PinLaterLookupJobRequest(Lists.newArrayList(jobDesc));
    lookupJobRequest.setIncludeBody(true);
    PinLaterJobInfo jobInfo = getBackend().lookupJobs(lookupJobRequest).get().get(jobDesc);
    Assert.assertEquals(PinLaterJobState.PENDING, jobInfo.getJobState());
    Assert.assertEquals("old_body", new String(jobInfo.getBody()));

    // Dequeue the job so it's in a running state.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    getBackend().dequeueJobs("test-dequeuer", dequeueRequest).get();

    // Try to checkpoint the job but as a worker who is not present in the claim descriptor field.
    // This operation should be a no-op.
    checkpointJobRequest = new PinLaterCheckpointJobRequest(jobDesc);
    checkpointJobRequest.setNewBody("new_body");
    checkpointJobRequest.setMoveToPending(true);
    checkpointJobsRequest = new PinLaterCheckpointJobsRequest(
        getQueueName(), Lists.newArrayList(checkpointJobRequest));
    getBackend().checkpointJobs("really_slow_worker", checkpointJobsRequest).get();
    // Verify that the job is still in progress and has not changed.
    lookupJobRequest = new PinLaterLookupJobRequest(Lists.newArrayList(jobDesc));
    lookupJobRequest.setIncludeBody(true);
    jobInfo = getBackend().lookupJobs(lookupJobRequest).get().get(jobDesc);
    Assert.assertEquals(PinLaterJobState.IN_PROGRESS, jobInfo.getJobState());
    Assert.assertEquals("old_body", new String(jobInfo.getBody()));
  }

  @Test
  public void testCheckpointJobNewAttempts() {
    // Enqueue a job.
    long runAfterMillis = System.currentTimeMillis();
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("old_body".getBytes()));
    job.setRunAfterTimestampMillis(runAfterMillis);
    enqueueRequest.addToJobs(job);
    String jobDesc = getBackend().enqueueJobs(enqueueRequest).get().getJobDescriptors().get(0);

    // Dequeue the job so it's in a running state.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    getBackend().dequeueJobs("test-dequeuer", dequeueRequest).get();

    // Try to checkpoint one of the jobs and reset the number of attempts allowed.
    PinLaterCheckpointJobRequest checkpointJobRequest = new PinLaterCheckpointJobRequest(jobDesc);
    checkpointJobRequest.setNumOfAttemptsAllowed(5);
    PinLaterCheckpointJobsRequest checkpointJobsRequest = new PinLaterCheckpointJobsRequest(
        getQueueName(), Lists.newArrayList(checkpointJobRequest));
    getBackend().checkpointJobs("test-dequeuer", checkpointJobsRequest).get();

    // Get the current attempts allowed and then verify that checkpointing reset the
    // job's attempts allowed but kept all other fields the same.
    PinLaterLookupJobRequest lookupJobRequest =
        new PinLaterLookupJobRequest(Lists.newArrayList(jobDesc));
    lookupJobRequest.setIncludeBody(true);
    PinLaterJobInfo jobInfo = getBackend().lookupJobs(lookupJobRequest).get().get(jobDesc);

    Assert.assertEquals(5, jobInfo.getAttemptsAllowed());
    Assert.assertEquals(5, jobInfo.getAttemptsRemaining());
    Assert.assertEquals(PinLaterJobState.IN_PROGRESS, jobInfo.getJobState());
    Assert.assertEquals("old_body", new String(jobInfo.getBody()));
    Assert.assertEquals("", jobInfo.getCustomStatus());
  }

  @Test
  public void testCheckpointJobPrependCustomStatus() {
    // Enqueue a job.
    long runAfterMillis = System.currentTimeMillis();
    PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
    enqueueRequest.setQueueName(getQueueName());
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap("old_body".getBytes()));
    job.setRunAfterTimestampMillis(runAfterMillis);
    enqueueRequest.addToJobs(job);
    String jobDesc = getBackend().enqueueJobs(enqueueRequest).get().getJobDescriptors().get(0);

    // Dequeue the job so it's in a running state.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 1);
    getBackend().dequeueJobs("test-dequeuer", dequeueRequest).get();

    // Try to checkpoint one of the jobs and append message to custom status.
    PinLaterCheckpointJobRequest checkpointJobRequest = new PinLaterCheckpointJobRequest(jobDesc);
    checkpointJobRequest.setPrependCustomStatus("new_custom_status1\n");
    PinLaterCheckpointJobsRequest checkpointJobsRequest = new PinLaterCheckpointJobsRequest(
        getQueueName(), Lists.newArrayList(checkpointJobRequest));
    getBackend().checkpointJobs("test-dequeuer", checkpointJobsRequest).get();

    checkpointJobRequest.setPrependCustomStatus("new_custom_status2\n");
    getBackend().checkpointJobs("test-dequeuer", checkpointJobsRequest).get();

    // Get the current custom status and then verify that checkpointing appends the
    // message to the jobs custom status but kept all other fields the same.
    PinLaterLookupJobRequest lookupJobRequest =
        new PinLaterLookupJobRequest(Lists.newArrayList(jobDesc));
    lookupJobRequest.setIncludeBody(true);
    PinLaterJobInfo jobInfo = getBackend().lookupJobs(lookupJobRequest).get().get(jobDesc);

    Assert.assertEquals("new_custom_status2\nnew_custom_status1\n", jobInfo.getCustomStatus());
    Assert.assertEquals(PinLaterJobState.IN_PROGRESS, jobInfo.getJobState());
    Assert.assertEquals("old_body", new String(jobInfo.getBody()));
  }
}
