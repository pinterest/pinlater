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
import com.pinterest.pinlater.thrift.PinLaterDequeueRequest;
import com.pinterest.pinlater.thrift.PinLaterDequeueResponse;
import com.pinterest.pinlater.thrift.PinLaterEnqueueRequest;
import com.pinterest.pinlater.thrift.PinLaterEnqueueResponse;
import com.pinterest.pinlater.thrift.PinLaterJob;
import com.pinterest.pinlater.thrift.PinLaterJobAckInfo;
import com.pinterest.pinlater.thrift.PinLaterJobAckRequest;
import com.pinterest.pinlater.thrift.PinLaterJobInfo;
import com.pinterest.pinlater.thrift.PinLaterJobState;
import com.pinterest.pinlater.thrift.PinLaterLookupJobRequest;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public abstract class BackendQueueMonitorBaseTest<T1 extends PinLaterBackendBase,
    T2 extends BackendQueueMonitorBase> {

  private static final Random RANDOM = new Random();

  protected abstract String getQueueName();

  protected abstract T1 getBackend();

  /**
   * Creates a queue monitor with custom settings. We will invoke this ourselves.
   * Note that the backend will create a queue monitor on a separate thread too,
   * but the default configuration means it won't run for the duration of this test,
   * so we are fine.
   */
  protected abstract T2 createQueueMonitor(long jobClaimedTimeoutMillis,
                                           long jobSucceededGCTimeoutMillis,
                                           long jobFailedGCTimeoutMillis);

  @Test
  public void testJobGC() throws InterruptedException {
    List<String> jobDescriptors = Lists.newArrayList();

    // Enqueue 100 jobs with no retries and at random priorities.
    // We intentionally don't use a batch request, to ensure distribution across shards.
    for (int i = 0; i < 100; i++) {
      PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
      enqueueRequest.setQueueName(getQueueName());
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(new String("job_body_" + i).getBytes()));
      job.setNumAttemptsAllowed(1);
      job.setPriority((byte) (RANDOM.nextInt(3) + 1));
      enqueueRequest.addToJobs(job);
      PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
      Assert.assertEquals(1, enqueueResponse.getJobDescriptorsSize());
      jobDescriptors.add(enqueueResponse.getJobDescriptors().get(0));
    }

    // Lookup the jobs and validate state.
    PinLaterLookupJobRequest lookupJobRequest = new PinLaterLookupJobRequest();
    for (String jobDesc : jobDescriptors) {
      lookupJobRequest.addToJobDescriptors(jobDesc);
    }
    Map<String, PinLaterJobInfo> jobInfoMap = getBackend().lookupJobs(lookupJobRequest).get();
    Assert.assertEquals(100, jobInfoMap.size());
    for (PinLaterJobInfo jobInfo : jobInfoMap.values()) {
      Assert.assertEquals(PinLaterJobState.PENDING, jobInfo.getJobState());
    }

    // Dequeue all 100.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 100);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(100, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    // Lookup the jobs and validate state.
    jobInfoMap = getBackend().lookupJobs(lookupJobRequest).get();
    Assert.assertEquals(100, jobInfoMap.size());
    for (PinLaterJobInfo jobInfo : jobInfoMap.values()) {
      Assert.assertEquals(PinLaterJobState.IN_PROGRESS, jobInfo.getJobState());
    }

    // Ack 50 jobs as succeeded, 50 as failed.
    PinLaterJobAckRequest jobAckRequest = new PinLaterJobAckRequest(getQueueName());
    boolean alternate = false;
    for (String jobDesc : jobDescriptors) {
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

    // Backend timestamps are in second resolution, so one second is the minimum we'll need
    // to sleep to ensure this test works.
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));

    // Run queue monitor configured with low succeeded GC timeout.
    createQueueMonitor(TimeUnit.HOURS.toMillis(1), 1, TimeUnit.HOURS.toMillis(1)).run();

    // All succeeded jobs should have been GC'ed, but not failed jobs.
    Assert.assertEquals(0, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.SUCCEEDED));
    Assert.assertEquals(50, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.FAILED));

    // Run queue monitor configured with low failed GC timeout.
    createQueueMonitor(TimeUnit.HOURS.toMillis(1), TimeUnit.HOURS.toMillis(1), 1).run();

    // Now failed jobs should also be GC'ed.
    Assert.assertEquals(0, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.FAILED));

    // Looking up the jobs should return an empty map.
    jobInfoMap = getBackend().lookupJobs(lookupJobRequest).get();
    Assert.assertEquals(0, jobInfoMap.size());
  }

  @Test
  public void testJobClaimedTimeout() throws InterruptedException {
    // Enqueue 100 jobs at random priorities, 50 of them with no retries, and 50 with 1 retry.
    // We intentionally don't use a batch request, to ensure distribution across shards.
    for (int i = 0; i < 100; i++) {
      PinLaterEnqueueRequest enqueueRequest = new PinLaterEnqueueRequest();
      enqueueRequest.setQueueName(getQueueName());
      PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(new String("job_body_" + i).getBytes()));
      job.setNumAttemptsAllowed(i < 50 ? 1 : 2);
      job.setPriority((byte) (RANDOM.nextInt(3) + 1));
      enqueueRequest.addToJobs(job);
      PinLaterEnqueueResponse enqueueResponse = getBackend().enqueueJobs(enqueueRequest).get();
      Assert.assertEquals(1, enqueueResponse.getJobDescriptorsSize());
    }

    // Dequeue all 100.
    PinLaterDequeueRequest dequeueRequest = new PinLaterDequeueRequest(getQueueName(), 100);
    PinLaterDequeueResponse
        dequeueResponse =
        getBackend().dequeueJobs("test", dequeueRequest).get();
    Assert.assertEquals(100, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.IN_PROGRESS));

    // Backend timestamps are in second resolution, so one second is the minimum we'll need
    // to sleep to ensure this test works.
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));

    // Run queue monitor configured with low job claimed timeout.
    createQueueMonitor(1, TimeUnit.HOURS.toMillis(1), TimeUnit.HOURS.toMillis(1)).run();

    // 50 jobs should go to PENDING, remaining 50 should go to FAILED.
    Assert.assertEquals(50, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.PENDING));
    Assert.assertEquals(50, PinLaterTestUtils.getJobCount(getBackend(), getQueueName(),
        PinLaterJobState.FAILED));
  }
}
