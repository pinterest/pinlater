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
package com.pinterest.pinlater;

import com.pinterest.pinlater.backends.common.PinLaterBackendUtils;
import com.pinterest.pinlater.backends.common.PinLaterJobDescriptor;
import com.pinterest.pinlater.commons.config.ConfigFileWatcher;
import com.pinterest.pinlater.thrift.Constants;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.collections.Pair;
import com.twitter.concurrent.AsyncSemaphore;
import com.twitter.concurrent.Permit;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.Try;
import org.apache.commons.configuration.PropertiesConfiguration;
import scala.Tuple2;
import scala.runtime.BoxedUnit;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PinLater backend base, which handles the FuturePool and batch logic for each PinLater thrift
 * service API. The operations to the backend should be implemented in the class which inherits
 * this base class.
 *
 * This base class is intended for backend class which does blocking operations to its backend. If
 * the request to the backend is async itself, then the backend class should not inherit this base
 * class.
 */
public abstract class PinLaterBackendBase implements PinLaterBackendIface {

  protected static final Random RANDOM = new Random();

  private final AtomicLong claimSuffix = new AtomicLong(0);
  private final PropertiesConfiguration configuration;
  private final String serverHostName;
  private final long serverStartTimeMillis;
  private final int queryParallelism;
  private final int numAutoRetries;
  private final String backendName;
  private final String shardConfigFilePath;
  // All the queues in the same cluster share the priority levels. Priority level starts from
  // 1 (highest) to ``NUM_PRIORITY_LEVELS`` (lowest). The number of priority levels should be kept
  // as small as possible to make backend best performance. Typically it should not be greater
  // than 3.
  protected final int numPriorityLevels;
  protected ExecutorServiceFuturePool futurePool;
  private LoadingCache<String, AsyncSemaphore> dequeueSemaphoreMap;

  public PinLaterBackendBase(PropertiesConfiguration configuration,
                             String backendName,
                             String serverHostName,
                             long serverStartTimeMillis) {
    this.configuration = Preconditions.checkNotNull(configuration);
    this.backendName = MorePreconditions.checkNotBlank(backendName);
    this.serverHostName = MorePreconditions.checkNotBlank(serverHostName);
    this.serverStartTimeMillis = serverStartTimeMillis;
    this.queryParallelism = configuration.getInt("BACKEND_QUERY_PARALLELISM");
    Preconditions.checkArgument(queryParallelism > 0);
    this.numAutoRetries = configuration.getInt("BACKEND_NUM_AUTO_RETRIES");
    this.numPriorityLevels = configuration.getInt("NUM_PRIORITY_LEVELS");
    Preconditions.checkArgument(numPriorityLevels >= 1);
    this.shardConfigFilePath = System.getProperty("backend_config");
  }

  /*
   * After finishing its own initialization, each subclass needs to call this function to
   * register zk config update callback function and initialize the futurePool and dequeue
   * semaphoreMap.
   */
  protected void initialize() throws Exception {
    if (this.shardConfigFilePath != null) {
      String fullFilePath = getClass().getResource("/" + shardConfigFilePath).getPath();
      ConfigFileWatcher.defaultInstance().addWatch(
          fullFilePath, new ExceptionalFunction<byte[], Void>() {
        @Override
        public synchronized Void applyE(byte[] bytes) throws Exception {
          processConfigUpdate(bytes);
          return null;
        }
      });
    }

    // Initialize the future pool we will use to make blocking calls to Redis.
    // We size the future pool such that there is one thread for every available connection.
    int futurePoolSize = configuration.getInt("BACKEND_CONNECTIONS_PER_SHARD") * getShards().size();
    this.futurePool = new ExecutorServiceFuturePool(Executors.newFixedThreadPool(
        futurePoolSize,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(
            backendName + "FuturePool-%d").build()));

    // Create a map of queueName -> aync semaphore to control dequeue concurrency.
    // We configure the map to create entries on demand since queues can be created at any time.
    final int dequeueConcurrencyPerQueue =
        configuration.getInt("BACKEND_DEQUEUE_CONCURRENCY_PER_QUEUE_PER_SHARD") * getShards()
            .size();
    // We set maxWaiters on the async semaphore to the max concurrency on the server as an
    // additional safety measure.
    final int maxWaiters = configuration.getInt("MAX_CONCURRENT_REQUESTS");
    this.dequeueSemaphoreMap = CacheBuilder.newBuilder().build(
        new CacheLoader<String, AsyncSemaphore>() {
          @Override
          public AsyncSemaphore load(String queueName) throws Exception {
            AsyncSemaphore asyncSemaphore =
                new AsyncSemaphore(dequeueConcurrencyPerQueue, maxWaiters);
            Stats.setGauge("dequeue-semaphore-waiters-" + queueName, asyncSemaphore.numWaiters());
            return asyncSemaphore;
          }
        });
  }

  /*
   * Returns all the shard ids of the backend.
   */
  protected abstract ImmutableSet<String> getShards();

  /*
   * Create the queue on all of the shards.
   */
  protected abstract void createQueueImpl(final String queueName) throws Exception;

  /*
   * Delete the queue on all of the shards.
   */
  protected abstract void deleteQueueImpl(final String queueName) throws Exception;

  /*
   * Enqueue a single job to the given queue.
   */
  protected abstract String enqueueSingleJob(
      final String queueName,
      final PinLaterJob job,
      final int numAutoRetries) throws Exception;

  /*
   * Dequeue up to ``jobsNeeded`` number of jobs from the ``priority`` queue in the given shard.
   */
  protected abstract PinLaterDequeueResponse dequeueJobsFromShard(
      final String queueName,
      final String shardName,
      final int priority,
      final String claimDescriptor,
      final int jobsNeeded,
      final int numAutoRetries,
      final boolean dryRun) throws Exception;

  /*
   * Ack a single job from the given queue.
   */
  protected abstract void ackSingleJob(
      final String queueName,
      final boolean succeeded,
      final PinLaterJobAckInfo jobAckInfo,
      final int numAutoRetries) throws Exception;

  /*
   * Checkpoint a single job from the given queue.
   */
  protected abstract void checkpointSingleJob(
      final String source,
      final String queueName,
      final PinLaterCheckpointJobRequest request,
      final int numAutoRetries) throws Exception;

  /*
   * Look up the job from the ``priority`` queue in the given shard.
   */
  protected abstract PinLaterJobInfo lookupJobFromShard(
      final String queueName,
      final String shardName,
      final int priority,
      final long localId,
      final boolean isIncludeBody) throws Exception;

  /*
   * Get the count of jobs from the ``priorities`` queue with ``jobState`` in the given shard.
   */
  protected abstract int getJobCountFromShard(
      final String queueName,
      final String shardName,
      final Set<Integer> priorities,
      final PinLaterJobState jobState,
      final boolean countFutureJobs,
      final String bodyRegexToMatch) throws Exception;

  /*
   * Get the names of all existing queues.
   */
  protected abstract Set<String> getQueueNamesImpl() throws Exception;

  /*
   * Grab job information from a shard sorted by most recent first based on run_after,
   * where the jobs have the specified priority and job state.
   */
  protected abstract List<PinLaterJobInfo> scanJobsFromShard(
      final String queueName,
      final String shardName,
      final Set<Integer> priorities,
      final PinLaterJobState jobState,
      final boolean scanFutureJobs,
      final String continuation,
      final int limit,
      final String bodyRegexToMatch) throws Exception;

  /*
   * Retry some failed jobs in a particular queue. Returns the number of failed jobs that were
   * granted retries.
   */
  protected abstract int retryFailedJobsFromShard(
      final String queueName,
      final String shardName,
      final int priority,
      final int attemptsRemaining,
      final long runAfterTimestampMillis,
      final int limit) throws Exception;

  /*
   * Delete some jobs from a particular queue. Returns the number of failed jobs that were deleted.
   */
  protected abstract int deleteJobsFromShard(
      final String queueName,
      final String shardName,
      final PinLaterJobState jobState,
      final int priority,
      final String bodyRegexToMatch,
      final int limit) throws Exception;

  /*
   * Update shard map when zk config file changed. This function doesn't need to be synchronized
   * because ConfigFileWatcher ensures only one update is active at any time.
   */
  protected abstract void processConfigUpdate(byte[] bytes) throws Exception;

  public Future<Void> createQueue(final String name) {
    return futurePool.apply(new ExceptionalFunction0<Void>() {
      @Override
      public Void applyE() throws Throwable {
        createQueueImpl(name);
        return null;
      }
    });
  }

  public Future<Void> deleteQueue(final String name, final String password) {
    String passwordHash;
    try {
      passwordHash = PinLaterBackendUtils.getSaltedHash(password);
    } catch (NoSuchAlgorithmException e) {
      return Future.exception(
          new PinLaterException(ErrorCode.UNKNOWN, "Error finding hashing algorithm."));
    }
    if (!passwordHash.equals(configuration.getString("ADMIN_PASSWORD_HASH"))) {
      return Future.exception(
          new PinLaterException(ErrorCode.PASSWORD_INVALID, "Invalid admin password."));
    }

    return futurePool.apply(new ExceptionalFunction0<Void>() {
      @Override
      public Void applyE() throws Throwable {
        deleteQueueImpl(name);
        return null;
      }
    });
  }

  /**
   * Identical to deleteQueue method above, but with no password and intended for testing use only.
   */
  @VisibleForTesting
  public Future<Void> deleteQueue(final String name) {
    return futurePool.apply(new ExceptionalFunction0<Void>() {
      @Override
      public Void applyE() throws Throwable {
        deleteQueueImpl(name);
        return null;
      }
    });
  }

  public Future<Integer> getJobCount(final PinLaterGetJobCountRequest request) {
    // If no priority is specified, search for jobs of all priorities.
    Range<Integer> priorityRange = request.isSetPriority()
                                   ? Range.closed((int) request.getPriority(),
        (int) request.getPriority()) :
                                   Range.closed(1, numPriorityLevels);
    final ContiguousSet<Integer> priorities =
        ContiguousSet.create(priorityRange, DiscreteDomain.integers());

    // Execute count query on each shard in parallel.
    List<Future<Integer>> futures = Lists.newArrayListWithCapacity(getShards().size());
    for (final String shardName : getShards()) {
      futures.add(futurePool.apply(new ExceptionalFunction0<Integer>() {
        @Override
        public Integer applyE() throws Throwable {
          return getJobCountFromShard(
              request.getQueueName(),
              shardName,
              priorities,
              request.getJobState(),
              request.isCountFutureJobs(),
              request.getBodyRegexToMatch());
        }
      }));
    }

    return Future.collect(futures).map(
        new Function<List<Integer>, Integer>() {
          @Override
          public Integer apply(List<Integer> shardCounts) {
            int totalCount = 0;
            for (Integer shardCount : shardCounts) {
              totalCount += shardCount;
            }
            return totalCount;
          }
        });
  }

  public Future<PinLaterEnqueueResponse> enqueueJobs(final PinLaterEnqueueRequest request) {
    // Partition the jobs in the enqueue request such that there are roughly
    // <queryParallelism> partitions. Then execute those in parallel. Within each partition,
    // enqueues are executed serially.
    List<Future<PinLaterEnqueueResponse>> futures = PinLaterBackendUtils.executePartitioned(
        request.getJobs(),
        queryParallelism,
        new Function<List<PinLaterJob>, Future<PinLaterEnqueueResponse>>() {
          @Override
          public Future<PinLaterEnqueueResponse> apply(final List<PinLaterJob> jobs) {
            return futurePool.apply(new ExceptionalFunction0<PinLaterEnqueueResponse>() {
              @Override
              public PinLaterEnqueueResponse applyE() throws Throwable {
                PinLaterEnqueueResponse response = new PinLaterEnqueueResponse();
                for (PinLaterJob job : jobs) {
                  // Collect stats around job body size.
                  Stats.addMetric("job_body_size_" + request.getQueueName(), job.getBody().length);
                  Stats.addMetric("job-body-size", job.getBody().length);
                  // Check whether the priority is supported.
                  if (job.getPriority() > numPriorityLevels || job.getPriority() < 1) {
                    Stats.incr(String.format("%s-priority-not-supported-enqueue", backendName));
                    throw new PinLaterException(ErrorCode.PRIORITY_NOT_SUPPORTED,
                        String.valueOf(job.getPriority()));
                  }
                  response.addToJobDescriptors(enqueueSingleJob(
                      request.getQueueName(), job, numAutoRetries));
                }
                return response;
              }
            });
          }
        });

    return Future.collect(futures).map(
        new Function<List<PinLaterEnqueueResponse>, PinLaterEnqueueResponse>() {
          @Override
          public PinLaterEnqueueResponse apply(List<PinLaterEnqueueResponse> subResponses) {
            PinLaterEnqueueResponse response =
                new PinLaterEnqueueResponse(Lists.<String>newArrayList());
            for (PinLaterEnqueueResponse subResponse : subResponses) {
              response.getJobDescriptors().addAll(subResponse.getJobDescriptors());
            }
            return response;
          }
        });
  }

  public Future<PinLaterDequeueResponse> dequeueJobs(final String source,
                                                     final PinLaterDequeueRequest request) {
    Future<PinLaterDequeueResponse> dequeueFuture;
    try {
      dequeueFuture = dequeueSemaphoreMap.get(request.getQueueName()).acquire().flatMap(
          new Function<Permit, Future<PinLaterDequeueResponse>>() {
            @Override
            public Future<PinLaterDequeueResponse> apply(final Permit permit) {
              return futurePool.apply(new ExceptionalFunction0<PinLaterDequeueResponse>() {
                @Override
                public PinLaterDequeueResponse applyE() throws Throwable {
                  return dequeueJobsImpl(source, request, numAutoRetries);
                }
              }).respond(new Function<Try<PinLaterDequeueResponse>, BoxedUnit>() {
                @Override
                public BoxedUnit apply(Try<PinLaterDequeueResponse> responseTry) {
                  permit.release();
                  return BoxedUnit.UNIT;
                }
              });
            }
          });
    } catch (ExecutionException e) {
      // The dequeueSemaphoreMap's get() can in theory throw an ExecutionException, but we
      // never expect it in practice since our load method is simply new'ing up an AsyncSemaphore.
      dequeueFuture = Future.exception(e);
    }

    // Dequeue requests can contain ack requests as payloads. If so, we execute both in parallel.
    Future<Void> ackFuture = request.isSetJobAckRequest()
                             ? ackDequeuedJobsImpl(request.getJobAckRequest()) : Future.Void();

    return dequeueFuture.join(ackFuture).map(
        new Function<Tuple2<PinLaterDequeueResponse, Void>, PinLaterDequeueResponse>() {
          @Override
          public PinLaterDequeueResponse apply(Tuple2<PinLaterDequeueResponse, Void> tuple) {
            return tuple._1();
          }
        });
  }

  public Future<Map<String, PinLaterJobInfo>> lookupJobs(final PinLaterLookupJobRequest request) {
    List<Future<Pair<String, PinLaterJobInfo>>> lookupJobFutures =
        Lists.newArrayListWithCapacity(request.getJobDescriptorsSize());
    for (final String jobDescriptor : request.getJobDescriptors()) {
      Future<Pair<String, PinLaterJobInfo>> lookupJobFuture = futurePool.apply(
          new ExceptionalFunction0<Pair<String, PinLaterJobInfo>>() {
            @Override
            public Pair<String, PinLaterJobInfo> applyE() throws Throwable {
              PinLaterJobDescriptor jobDesc = new PinLaterJobDescriptor(jobDescriptor);
              PinLaterJobInfo jobInfo = lookupJobFromShard(
                  jobDesc.getQueueName(),
                  jobDesc.getShardName(),
                  jobDesc.getPriority(),
                  jobDesc.getLocalId(),
                  request.isIncludeBody());
              return new Pair<String, PinLaterJobInfo>(jobDescriptor, jobInfo);
            }
          });
      lookupJobFutures.add(lookupJobFuture);
    }

    return Future.collect(lookupJobFutures).map(
        new Function<List<Pair<String, PinLaterJobInfo>>, Map<String, PinLaterJobInfo>>() {
          @Override
          public Map<String, PinLaterJobInfo> apply(List<Pair<String, PinLaterJobInfo>> jobPairs) {
            Map<String, PinLaterJobInfo> lookupJobMap = Maps.newHashMap();
            for (Pair<String, PinLaterJobInfo> jobPair : jobPairs) {
              if (jobPair.getSecond() != null) {
                lookupJobMap.put(jobPair.getFirst(), jobPair.getSecond());
              }
            }
            return lookupJobMap;
          }
        });
  }

  public Future<Void> ackDequeuedJobs(final PinLaterJobAckRequest request) {
    return ackDequeuedJobsImpl(request);
  }

  public Future<Void> checkpointJobs(final String source,
                                     final PinLaterCheckpointJobsRequest request) {
    // Partition the requests such that there are roughly <queryParallelism> partitions. Then
    // execute those in parallel. Within each partition, each checkpoint is executed serially.
    List<Future<Void>> futures = Lists.newArrayList();
    if (request.getRequestsSize() > 0) {
      futures.addAll(PinLaterBackendUtils.executePartitioned(
          request.getRequests(),
          queryParallelism,
          new Function<List<PinLaterCheckpointJobRequest>, Future<Void>>() {
            @Override
            public Future<Void> apply(final List<PinLaterCheckpointJobRequest> checkpointRequests) {
              return futurePool.apply(new ExceptionalFunction0<Void>() {
                @Override
                public Void applyE() throws Throwable {
                  for (PinLaterCheckpointJobRequest checkpointRequest : checkpointRequests) {
                    checkpointSingleJob(source, request.getQueueName(), checkpointRequest,
                        numAutoRetries);
                  }
                  return null;
                }
              });
            }
          }));
    }
    return Future.collect(futures).voided();
  }

  public Future<Set<String>> getQueueNames() {
    return futurePool.apply(new ExceptionalFunction0<Set<String>>() {
      @Override
      public Set<String> applyE() throws Throwable {
        return getQueueNamesImpl();
      }
    });
  }

  public Future<PinLaterScanJobsResponse> scanJobs(final PinLaterScanJobsRequest request) {
    // Validate continuation token. CONTINUATION_START is the only supported token right now.
    if (!request.getContinuation().equals(Constants.CONTINUATION_START)) {
      return Future.exception(new PinLaterException(ErrorCode.CONTINUATION_INVALID,
          "CONTINUATION_START is the only continuation token supported right now."));
    }

    // If no priority is specified, search for jobs of all priorities.
    Range<Integer> priorityRange = request.isSetPriority()
                                   ? Range.closed((int) request.getPriority(),
        (int) request.getPriority()) :
                                   Range.closed(1, numPriorityLevels);
    final ContiguousSet<Integer> priorities =
        ContiguousSet.create(priorityRange, DiscreteDomain.integers());

    // Execute scanJobs query on each shard in parallel.
    List<Future<List<PinLaterJobInfo>>> futures =
        Lists.newArrayListWithCapacity(getShards().size());
    for (final String shardName : getShards()) {
      futures.add(futurePool.apply(new ExceptionalFunction0<List<PinLaterJobInfo>>() {
        @Override
        public List<PinLaterJobInfo> applyE() throws Throwable {
          return scanJobsFromShard(
              request.getQueueName(),
              shardName,
              priorities,
              request.getJobState(),
              request.isScanFutureJobs(),
              request.getContinuation(),
              request.getLimit(),
              request.getBodyRegexToMatch());
        }
      }));
    }

    // Perform a merge, and then truncate at the requested limit.
    return Future.collect(futures).map(
        new Function<List<List<PinLaterJobInfo>>, PinLaterScanJobsResponse>() {
          @Override
          public PinLaterScanJobsResponse apply(List<List<PinLaterJobInfo>> shardLists) {
            // First grab all of the lists of job info and perform a merge on them.
            List<PinLaterJobInfo> mergedList = PinLaterBackendUtils.mergeIntoList(
                shardLists,
                PinLaterBackendUtils.JobInfoComparator.getInstance(),
                request.getLimit());

            // If we were to support continuation we would need to create and set the token here.
            // Right now, we just leave it as the default: CONTINUATION_END.
            return new PinLaterScanJobsResponse(mergedList);
          }
        });
  }

  public Future<Integer> retryFailedJobs(final PinLaterRetryFailedJobsRequest request) {
    // Execute retryFailedJobs query on each shard until we have updated 'limit' number of jobs.
    return futurePool.apply(new ExceptionalFunction0<Integer>() {
      @Override
      public Integer applyE() throws Throwable {
        long currentTimeMillis = System.currentTimeMillis();
        int remainingLimit = request.getLimit();
        List<String> shardNames = getRandomShardShuffle();
        for (final String shardName : shardNames) {
          int numRetried = retryFailedJobsFromShard(
              request.getQueueName(),
              shardName,
              request.getPriority(),
              request.getAttemptsToAllow(),
              request.isSetRunAfterTimestampMillis()
              ? request.getRunAfterTimestampMillis() : currentTimeMillis,
              remainingLimit);
          remainingLimit -= numRetried;
          if (remainingLimit <= 0) {
            break;
          }
        }
        return request.getLimit() - remainingLimit;
      }
    });
  }

  public Future<Integer> deleteJobs(final PinLaterDeleteJobsRequest request) {
    // Execute deleteJobs query on each shard until we have updated 'limit' number of jobs.
    return futurePool.apply(new ExceptionalFunction0<Integer>() {
      @Override
      public Integer applyE() throws Throwable {
        int remainingLimit = request.getLimit();
        List<String> shardNames = getRandomShardShuffle();
        for (final String shardName : shardNames) {
          int numDeleted = deleteJobsFromShard(
              request.getQueueName(),
              shardName,
              request.getJobState(),
              request.getPriority(),
              request.getBodyRegexToMatch(),
              remainingLimit);
          remainingLimit -= numDeleted;

          if (remainingLimit <= 0) {
            break;
          }
        }
        return request.getLimit() - remainingLimit;
      }
    });
  }

  private PinLaterDequeueResponse dequeueJobsImpl(
      final String source,
      final PinLaterDequeueRequest request,
      int numAutoRetries) throws Exception {
    List<String> shardNames = getRandomShardShuffle().subList(
        0, (int) Math.round(getShards().size() * request.getCoverage()));
    PinLaterDequeueResponse response = new PinLaterDequeueResponse();
    String claimDescriptor = constructClaimDescriptor(source);
    int jobsNeeded = request.getLimit();

    // Dequeue jobs in priority order, iterating over all shards till the desired number of
    // jobs have been retrieved.
    for (int priority = 1; priority <= numPriorityLevels; priority++) {
      for (final String shardName : shardNames) {
        PinLaterDequeueResponse shardResponse = dequeueJobsFromShard(request.getQueueName(),
            shardName, priority, claimDescriptor, jobsNeeded, numAutoRetries, request.isDryRun());

        // Add this shard's response fields into the local response variable.
        Map<String, ByteBuffer> jobToBody = shardResponse.getJobs();
        if (jobToBody != null) {
          for (Map.Entry<String, ByteBuffer> jobAndBody : jobToBody.entrySet()) {
            response.putToJobs(jobAndBody.getKey(), jobAndBody.getValue());
          }
        }
        Map<String, PinLaterDequeueMetadata> jobToMetadata = shardResponse.getJobMetadata();
        if (jobToMetadata != null) {
          for (Map.Entry<String, PinLaterDequeueMetadata> jobAndMetadata :
              jobToMetadata.entrySet()) {
            response.putToJobMetadata(jobAndMetadata.getKey(), jobAndMetadata.getValue());
          }
        }

        jobsNeeded -= shardResponse.getJobsSize();
        if (jobsNeeded <= 0) {
          return response;
        }
      }
    }

    return response;
  }

  private Future<Void> ackDequeuedJobsImpl(final PinLaterJobAckRequest request) {
    // Partition the jobs such that there are roughly <queryParallelism> partitions. Then execute
    // those in parallel. Within each partition, acks are executed serially.

    List<Future<Void>> futures = Lists.newArrayList();

    // Succeeded jobs.
    if (request.getJobsSucceededSize() > 0) {
      futures.addAll(PinLaterBackendUtils.executePartitioned(
          request.getJobsSucceeded(),
          queryParallelism,
          new Function<List<PinLaterJobAckInfo>, Future<Void>>() {
            @Override
            public Future<Void> apply(final List<PinLaterJobAckInfo> jobAckInfos) {
              return futurePool.apply(new ExceptionalFunction0<Void>() {
                @Override
                public Void applyE() throws Throwable {
                  for (PinLaterJobAckInfo jobAckInfo : jobAckInfos) {
                    ackSingleJob(request.getQueueName(), true, jobAckInfo, numAutoRetries);
                  }
                  return null;
                }
              });
            }
          }));
    }

    // Failed jobs.
    if (request.getJobsFailedSize() > 0) {
      futures.addAll(PinLaterBackendUtils.executePartitioned(
          request.getJobsFailed(),
          queryParallelism,
          new Function<List<PinLaterJobAckInfo>, Future<Void>>() {
            @Override
            public Future<Void> apply(final List<PinLaterJobAckInfo> jobAckInfos) {
              return futurePool.apply(new ExceptionalFunction0<Void>() {
                @Override
                public Void applyE() throws Throwable {
                  for (PinLaterJobAckInfo jobAckInfo : jobAckInfos) {
                    ackSingleJob(request.getQueueName(), false, jobAckInfo, numAutoRetries);
                  }
                  return null;
                }
              });
            }
          }));
    }

    return Future.collect(futures).voided();
  }

  private List<String> getRandomShardShuffle() {
    List<String> shards = new ArrayList<String>();
    shards.addAll(getShards());
    Collections.shuffle(shards, RANDOM);
    return shards;
  }

  private String constructClaimDescriptor(String source) {
    return String.format("%s:%d:%d:%s:%d",
        serverHostName,
        configuration.getInt("THRIFT_PORT"),
        serverStartTimeMillis,
        source,
        claimSuffix.incrementAndGet());
  }
}
