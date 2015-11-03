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
package com.pinterest.pinlater.client;

import com.pinterest.pinlater.thrift.PinLater;
import com.pinterest.pinlater.thrift.PinLaterDequeueRequest;
import com.pinterest.pinlater.thrift.PinLaterDequeueResponse;
import com.pinterest.pinlater.thrift.PinLaterEnqueueRequest;
import com.pinterest.pinlater.thrift.PinLaterEnqueueResponse;
import com.pinterest.pinlater.thrift.PinLaterGetJobCountRequest;
import com.pinterest.pinlater.thrift.PinLaterJob;
import com.pinterest.pinlater.thrift.PinLaterJobAckInfo;
import com.pinterest.pinlater.thrift.PinLaterJobAckRequest;
import com.pinterest.pinlater.thrift.PinLaterJobState;
import com.pinterest.pinlater.thrift.PinLaterLookupJobRequest;
import com.pinterest.pinlater.thrift.RequestContext;

import com.google.common.base.Preconditions;
import com.twitter.util.Duration;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.Throw;
import com.twitter.util.Try;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class used by the PinLaterClientTool to issue requests to PinLater. This can be used for
 * correctness as well as load/performance testing.
 */
public class PinLaterQueryIssuer {

  private static final Logger LOG = LoggerFactory.getLogger(PinLaterQueryIssuer.class);
  private static final RequestContext REQUEST_CONTEXT;

  static {
    try {
      REQUEST_CONTEXT = new RequestContext(
          "pinlaterclienttool:" + InetAddress.getLocalHost().getHostName());
    } catch (UnknownHostException e) {
      LOG.error("Failed to initializer PinLaterQueryIssuer", e);
      throw new RuntimeException(e);
    }
  }

  private final Random random = new Random();
  private final RPCStatsLogger statsLogger = new RPCStatsLogger(1);

  private final CommandLine cmdLine;
  private final String queueName;
  private final String mode;
  private final int concurrency;
  private final int batchSize;
  private final long numQueries;
  private final int dequeueSuccessPercent;
  private final String jobDescriptor;
  private final byte priority;
  private final int jobState;
  private final boolean countFutureJobs;

  public PinLaterQueryIssuer(CommandLine cmdLine) {
    this.cmdLine = Preconditions.checkNotNull(cmdLine);
    this.mode = cmdLine.getOptionValue("mode");
    this.queueName = cmdLine.getOptionValue("queue", null);
    this.concurrency = Integer.parseInt(cmdLine.getOptionValue("concurrency", "1"));
    this.batchSize = Integer.parseInt(cmdLine.getOptionValue("batch_size", "1"));
    this.numQueries = Integer.parseInt(cmdLine.getOptionValue("num_queries", "1"));
    this.dequeueSuccessPercent = Integer.parseInt(
        cmdLine.getOptionValue("dequeue_success_percent", "100"));
    this.jobDescriptor = cmdLine.getOptionValue("job_descriptor", "");
    this.priority = Byte.parseByte(cmdLine.getOptionValue("priority", "1"));
    this.jobState = Integer.parseInt(cmdLine.getOptionValue("job_state", "0"));
    this.countFutureJobs = Boolean.parseBoolean(
        cmdLine.getOptionValue("count_future_jobs", "false"));
  }

  public void run() throws Exception {
    PinLaterClient client = new PinLaterClient(cmdLine);
    final PinLater.ServiceIface iface = client.getIface();

    if (mode.equals("create")) {
      createQueue(iface);
    } else if (mode.equals("check_dequeue")) {
      checkDequeue(iface);
    } else if (mode.equals("lookup")) {
      lookupJobs(iface);
    } else if (mode.equals("get_job_count")) {
      getJobCount(iface);
    } else if (mode.equals("get_queue_names")) {
      getQueueNames(iface);
    } else if (mode.equals("enqueue")) {
      issueEnqueueRequests(iface);
    } else {
      Preconditions.checkArgument(mode.equals("dequeue"));
      issueDequeueAckRequests(iface);
    }

    client.getService().release();
  }

  private void createQueue(PinLater.ServiceIface iface) {
    Preconditions.checkNotNull(queueName, "Queue was not specified.");
    iface.createQueue(REQUEST_CONTEXT, queueName).get();
    LOG.info("Created queue: " + queueName);
  }

  private void checkDequeue(PinLater.ServiceIface iface) {
    Preconditions.checkNotNull(queueName, "Queue was not specified.");
    PinLaterDequeueRequest getOneJobRequest = new PinLaterDequeueRequest(queueName, 1);
    getOneJobRequest.setDryRun(true);
    boolean jobsAvailable =
        iface.dequeueJobs(REQUEST_CONTEXT, getOneJobRequest).get().getJobsSize() != 0;
    LOG.info("Jobs available for dequeue: " + jobsAvailable);
  }

  private void lookupJobs(PinLater.ServiceIface iface) {
    Preconditions.checkNotNull(queueName, "Queue was not specified.");
    Preconditions.checkArgument(
        !jobDescriptor.isEmpty(), "No job descriptor specified to lookup");
    PinLaterLookupJobRequest lookupJobRequest = new PinLaterLookupJobRequest();
    lookupJobRequest.setIncludeBody(true);
    lookupJobRequest.addToJobDescriptors(jobDescriptor);
    LOG.info("Job: " + iface.lookupJobs(REQUEST_CONTEXT, lookupJobRequest).get());
  }

  private void getJobCount(PinLater.ServiceIface iface) {
    Preconditions.checkNotNull(queueName, "Queue was not specified.");
    Preconditions.checkNotNull(
        PinLaterJobState.findByValue(jobState), "Invalid job state specified.");
    PinLaterGetJobCountRequest getJobCountRequest = new PinLaterGetJobCountRequest();
    getJobCountRequest.setQueueName(queueName);
    getJobCountRequest.setJobState(PinLaterJobState.findByValue(jobState));
    if (cmdLine.hasOption("priority")) {
      getJobCountRequest.setPriority(priority);
    }
    getJobCountRequest.setCountFutureJobs(countFutureJobs);
    LOG.info("# jobs: " + iface.getJobCount(REQUEST_CONTEXT, getJobCountRequest).get());
  }

  private void getQueueNames(PinLater.ServiceIface iface) {
    Set<String> queueNames = iface.getQueueNames(REQUEST_CONTEXT).get();
    LOG.info("Queues: " + queueNames);
  }

  private void issueEnqueueRequests(PinLater.ServiceIface iface) throws InterruptedException {
    Preconditions.checkNotNull(queueName, "Queue was not specified.");
    final AtomicLong queriesIssued = new AtomicLong(0);
    final Semaphore permits = new Semaphore(concurrency);
    while (numQueries == -1 || queriesIssued.get() < numQueries) {
      final PinLaterEnqueueRequest request = new PinLaterEnqueueRequest();
      request.setQueueName(queueName);
      for (int i = 0; i < batchSize; i++) {
        PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(
            new String("task_" + random.nextInt(Integer.MAX_VALUE)).getBytes()));
        job.setPriority(priority);
        request.addToJobs(job);
      }
      final long startTimeNanos = System.nanoTime();
      queriesIssued.incrementAndGet();
      permits.acquire();
      iface.enqueueJobs(REQUEST_CONTEXT, request).respond(
          new Function<Try<PinLaterEnqueueResponse>, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Try<PinLaterEnqueueResponse> responseTry) {
              permits.release();
              statsLogger.requestComplete(
                  Duration.fromNanoseconds(System.nanoTime() - startTimeNanos));
              if (responseTry.isThrow()) {
                LOG.info("Exception for request: " + request + " : " + ((Throw) responseTry).e());
              }
              return BoxedUnit.UNIT;
            }
          });
    }
    permits.acquire(concurrency);
    LOG.info("Enqueue queries issued: " + queriesIssued);
  }

  private void issueDequeueAckRequests(final PinLater.ServiceIface iface)
      throws InterruptedException {
    Preconditions.checkNotNull(queueName, "Queue was not specified.");
    final AtomicLong queriesIssued = new AtomicLong(0);
    final Semaphore permits = new Semaphore(concurrency);
    while (numQueries == -1 || queriesIssued.get() < numQueries) {
      final PinLaterDequeueRequest request = new PinLaterDequeueRequest();
      request.setQueueName(queueName);
      request.setLimit(batchSize);
      final long startTimeNanos = System.nanoTime();
      queriesIssued.incrementAndGet();
      permits.acquire();
      iface.dequeueJobs(REQUEST_CONTEXT, request).flatMap(
          new Function<PinLaterDequeueResponse, Future<Void>>() {
            @Override
            public Future<Void> apply(PinLaterDequeueResponse response) {
              if (response.getJobsSize() == 0) {
                return Future.Void();
              }

              PinLaterJobAckRequest jobAckRequest = new PinLaterJobAckRequest(queueName);
              for (String job : response.getJobs().keySet()) {
                if (random.nextInt(100) < dequeueSuccessPercent) {
                  jobAckRequest.addToJobsSucceeded(new PinLaterJobAckInfo(job));
                } else {
                  jobAckRequest.addToJobsFailed(new PinLaterJobAckInfo(job));
                }
              }
              return iface.ackDequeuedJobs(REQUEST_CONTEXT, jobAckRequest);
            }
          }).respond(new Function<Try<Void>, BoxedUnit>() {
        @Override
        public BoxedUnit apply(Try<Void> voidTry) {
          permits.release();
          statsLogger.requestComplete(
              Duration.fromNanoseconds(System.nanoTime() - startTimeNanos));
          if (voidTry.isThrow()) {
            LOG.info("Exception for request: " + request + " : " + ((Throw) voidTry).e());
          }
          return BoxedUnit.UNIT;
        }
      });
    }
    permits.acquire(concurrency);
    LOG.info("Dequeue/ack queries issued: " + queriesIssued);
  }
}
