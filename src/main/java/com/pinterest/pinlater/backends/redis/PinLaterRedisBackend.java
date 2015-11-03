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

import com.pinterest.pinlater.PinLaterBackendBase;
import com.pinterest.pinlater.backends.common.PinLaterBackendUtils;
import com.pinterest.pinlater.backends.common.PinLaterJobDescriptor;
import com.pinterest.pinlater.commons.healthcheck.HealthChecker;
import com.pinterest.pinlater.commons.util.BytesUtil;
import com.pinterest.pinlater.thrift.ErrorCode;
import com.pinterest.pinlater.thrift.PinLaterCheckpointJobRequest;
import com.pinterest.pinlater.thrift.PinLaterDequeueMetadata;
import com.pinterest.pinlater.thrift.PinLaterDequeueResponse;
import com.pinterest.pinlater.thrift.PinLaterException;
import com.pinterest.pinlater.thrift.PinLaterJob;
import com.pinterest.pinlater.thrift.PinLaterJobAckInfo;
import com.pinterest.pinlater.thrift.PinLaterJobInfo;
import com.pinterest.pinlater.thrift.PinLaterJobState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.Function;
import com.twitter.util.Future;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * PinLater backend implementation that uses redis as the underlying store.
 *
 * There are mainly three data structures used in the redis backend.
 *  1. string: used to incrementally get the next local id.
 *  2. sorted set:
 *     (1) used to store the jobIds under one individual priority and state queue, which
 *     means for each queue in one shard, we have ``PRIORITY_NUM`` * ``JOB_STATE_NUM`` sorted sets.
 *     The member in the sorted set is jobId, while the score is the time. The score in the pending
 *     queue means when to run the job, while in other queues, the score records when the job is put
 *     into the queue.
 *     (2) used to store all the queueNames under one sorted set on each shard. The score of this
 *     sorted set is the queue created time.
 *  3. hash: used to store the job information for each job. See the PINLATER_JOB_HASH_*_FIELD in
 *     RedisBackendUtils.
 *
 * We use both the queueName and shardId inside each of the above keys. That provides the benefits:
 *  1. One shard can have multiple queues(which is a very basic need).
 *  2. Two or more shards can be on the same redis instance with isolation guarantee.
 *
 * Backend specific behavior:
 *  1. Redis backend ensure that each appended custom status should not exceed
 *     ``RedisBackendUtils.CUSTOM_STATUS_SIZE_BYTES`` and should overwrite the previous custom
 *     status. This is to save memory in Redis.
 *  2. Redis backend checks queue existence only during enqueue. If not exists, it will raise
 *     QUEUE_NOT_FOUND exception.
 *  3. Redis uses LRU when memory hits limit. In our case, hash is always the first to be evicted
 *     since each job has its own hash while all jobs share the other data structures. Our redis
 *     backend code handles gracefully when job's hash is evicted while job id is still in job
 *     queue:
 *     - dequeue: Move the job from pending to in process queue. Let queue monitor to clean it up.
 *     - ack: Remove the job from pending queue.
 *     - scan: Use default value from ``RedisBackendUtils`` if not found from job hash.
 *     - lookup: Use default value from ``RedisBackendUtils`` if not found from job hash.
 *     - GC timeouted job: Remove the job from in progress queue.
 *     In other cases, our code does not depend on the job hash so we do not need to worry about.
 */
public class PinLaterRedisBackend extends PinLaterBackendBase {

  private static final Logger LOG = LoggerFactory.getLogger(PinLaterRedisBackend.class);

  private final ImmutableMap<String, RedisPools> shardMap;
  private final AtomicReference<ImmutableSet<String>> queueNames =
      new AtomicReference<ImmutableSet<String>>();
  private final HealthChecker healthChecker;

  /**
   * Creates an instance of the PinLaterRedisBackend.
   *
   * @param configuration configuration parameters for the backend.
   * @param redisConfigStream stream encapsulating the Redis json config.
   * @param serverHostName hostname of the PinLater server.
   * @param serverStartTimeMillis start time of the PinLater server.
   */
  public PinLaterRedisBackend(PropertiesConfiguration configuration,
                              InputStream redisConfigStream,
                              String serverHostName,
                              long serverStartTimeMillis) throws Exception {
    super(configuration, "Redis", serverHostName, serverStartTimeMillis);
    this.shardMap = RedisBackendUtils.buildShardMap(redisConfigStream, configuration);
    this.healthChecker = new HealthChecker("PinLaterRedis");
    for (RedisPools redisPools : shardMap.values()) {
      this.healthChecker.addServer(
          redisPools.getHost(),
          redisPools.getPort(),
          new RedisHeartBeater(new JedisClientHelper(), redisPools.getMonitorRedisPool()),
          configuration.getInt("REDIS_HEALTH_CHECK_CONSECUTIVE_FAILURES", 6),
          configuration.getInt("REDIS_HEALTH_CHECK_CONSECUTIVE_SUCCESSES", 6),
          configuration.getInt("REDIS_HEALTH_CHECK_PING_INTERVAL_SECONDS", 5),
          true);  // is live initially
    }

    // Start the JobQueueMonitor scheduled task.
    final int delaySeconds = configuration.getInt("BACKEND_MONITOR_THREAD_DELAY_SECONDS");
    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("RedisJobQueueMonitor-%d")
            .build());
    service.scheduleWithFixedDelay(
        new RedisQueueMonitor(shardMap, configuration, healthChecker),
        // Randomize initial delay to prevent all servers from running GC at the same time.
        delaySeconds + RANDOM.nextInt(delaySeconds),
        delaySeconds,
        TimeUnit.SECONDS);

    // Load queue names into memory. Silently catch exceptions to avoid failure in initialization.
    // If queue names are not loaded at this time, they will be retried upon requests.
    try {
      reloadQueueNames();
    } catch (Exception e) {
      // Retry the ack.
      Stats.incr("init-queuenames-failure");
      LOG.error("Failed to load queue names upon initialization.", e);
    }

    // Call Base class's initialization function to initialize the futurePool and dequeue
    // semaphoreMap.
    initialize();
  }

  @Override
  protected ImmutableSet<String> getShards() {
    return shardMap.keySet();
  }

  @Override
  protected void processConfigUpdate(byte[] bytes) {
    //TODO: Not yet implemented
  }

  @Override
  protected void createQueueImpl(final String queueName) throws Exception {
    // Add the queueName to the queueNames sorted set in each shard.
    final double currentTimeSeconds = System.currentTimeMillis() / 1000.0;
    for (final ImmutableMap.Entry<String, RedisPools> shard : shardMap.entrySet()) {
      final String queueNamesRedisKey = RedisBackendUtils.constructQueueNamesRedisKey(
          shard.getKey());
      RedisUtils.executeWithConnection(
          shard.getValue().getGeneralRedisPool(),
          new Function<Jedis, Void>() {
            @Override
            public Void apply(Jedis conn) {
              if (conn.zscore(queueNamesRedisKey, queueName) == null) {
                conn.zadd(queueNamesRedisKey, currentTimeSeconds, queueName);
              }
              return null;
            }
          });
    }
    reloadQueueNames();
  }

  @Override
  protected void deleteQueueImpl(final String queueName) throws Exception {
    for (final ImmutableMap.Entry<String, RedisPools> shard : shardMap.entrySet()) {
      final String queueNamesRedisKey = RedisBackendUtils.constructQueueNamesRedisKey(
          shard.getKey());
      RedisUtils.executeWithConnection(
          shard.getValue().getGeneralRedisPool(),
          new Function<Jedis, Void>() {
            @Override
            public Void apply(Jedis conn) {
              // We will delete the queue from the queueNames sorted set, and delete all the jobs
              // in the pending and in_progress queues
              // We intentionally do not delete the jobs in succeeded and failed queues to avoid
              // blocking redis. In the end, those jobs will be garbage collected.
              // There is chance that we have pending jobs again when there are indeed in progress
              // jobs and they get ack'ed as failure before we delete the in progress queue. But
              // since in practice, we won't delete queues until we know for sure no one is
              // enqueuing or dequeuing them, this is not an issue.
              conn.zrem(queueNamesRedisKey, queueName);
              List<PinLaterJobState> jobStatesToDelete = Lists.newArrayList(
                  PinLaterJobState.PENDING, PinLaterJobState.IN_PROGRESS);
              for (int priority = 1; priority <= numPriorityLevels; priority++) {
                for (PinLaterJobState jobState : jobStatesToDelete) {
                  String queueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                      queueName, shard.getKey(), priority, jobState);
                  String hashRedisKeyPrefix = RedisBackendUtils.constructHashRedisKeyPrefix(
                      queueName, shard.getKey());
                  List<String> keys = Lists.newArrayList(queueRedisKey, hashRedisKeyPrefix);
                  List<String> args = Lists.newArrayList();
                  conn.eval(RedisLuaScripts.DELETE_QUEUE, keys, args);
                }
              }
              return null;
            }
          });
    }
    reloadQueueNames();
  }

  @Override
  protected PinLaterJobInfo lookupJobFromShard(
      final String queueName,
      final String shardName,
      final int priority,
      final long localId,
      final boolean isIncludeBody) throws Exception {
    return RedisUtils.executeWithConnection(
        shardMap.get(shardName).getGeneralRedisPool(),
        new Function<Jedis, PinLaterJobInfo>() {
          @Override
          public PinLaterJobInfo apply(Jedis conn) {
            // Find out which state the job is in by querying each state.
            PinLaterJobState jobState = null;
            Double score = null;
            for (PinLaterJobState iterJobState : PinLaterJobState.values()) {
              String queueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                  queueName, shardName, priority, iterJobState);
              score = conn.zscore(queueRedisKey, String.valueOf(localId));
              if (score != null) {
                jobState = iterJobState;
                break;
              }
            }
            if (jobState == null || score == null) {
              return null;
            }

            PinLaterJobDescriptor jobDesc = new PinLaterJobDescriptor(
                queueName, shardName, priority, localId);
            PinLaterJobInfo jobInfo = new PinLaterJobInfo();
            jobInfo.setJobDescriptor(jobDesc.toString());
            jobInfo.setJobState(jobState);
            jobInfo.setRunAfterTimestampMillis((long) (score * 1000));

            // Get the job's attempts allowed, attempts remaining, custom status, created time,
            // updated time, claim descriptor (and body if needed) from job hash in redis.
            String hashRedisKey = RedisBackendUtils.constructHashRedisKey(
                queueName, shardName, localId);
            List<String> hashKeys = Lists.newArrayList(
                RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_ALLOWED_FIELD,
                RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD,
                RedisBackendUtils.PINLATER_JOB_HASH_CUSTOM_STATUS_FIELD,
                RedisBackendUtils.PINLATER_JOB_HASH_CREATED_AT_FIELD,
                RedisBackendUtils.PINLATER_JOB_HASH_UPDATED_AT_FIELD,
                RedisBackendUtils.PINLATER_JOB_HASH_CLAIM_DESCRIPTOR_FIELD);
            if (isIncludeBody) {
              hashKeys.add(RedisBackendUtils.PINLATER_JOB_HASH_BODY_FIELD);
            }
            List<String> jobRawInfo = conn.hmget(
                hashRedisKey, hashKeys.toArray(new String[hashKeys.size()]));
            // Fill up the rest of the jobInfo object and return it. If find the job's attempts
            // allow field is empty, probably the job hash has been evicted. Return null.
            if (jobRawInfo.get(0) != null) {
              jobInfo.setAttemptsAllowed(
                  RedisBackendUtils.parseJobHashAttemptsAllowed(jobRawInfo.get(0)));
              jobInfo.setAttemptsRemaining(
                  RedisBackendUtils.parseJobHashAttemptsRemaining(jobRawInfo.get(1)));
              jobInfo.setCustomStatus(
                  RedisBackendUtils.parseJobHashCustomStatus(jobRawInfo.get(2)));
              jobInfo.setCreatedAtTimestampMillis(
                  RedisBackendUtils.parseJobHashCreatedAt(jobRawInfo.get(3)));
              jobInfo.setUpdatedAtTimestampMillis(
                  RedisBackendUtils.parseJobHashUpdatedAt(jobRawInfo.get(4)));
              jobInfo.setClaimDescriptor(jobRawInfo.get(5));
              if (isIncludeBody) {
                jobInfo.setBody(BytesUtil.stringToByteBuffer(
                    RedisBackendUtils.parseJobHashBody(jobRawInfo.get(6))));
              }
              return jobInfo;
            } else {
              Stats.incr(String.format(
                  RedisBackendUtils.REDIS_JOB_HASH_NOT_FOUND_STATS_FORMAT,
                  queueName, shardName, priority, "lookup"));
              return null;
            }
          }
        });
  }

  @Override
  protected int getJobCountFromShard(
      final String queueName,
      final String shardName,
      final Set<Integer> priorities,
      final PinLaterJobState jobState,
      final boolean countFutureJobs,
      final String bodyRegexToMatch) throws Exception {
    // Skip the shard if it is unhealthy.
    if (!healthChecker.isServerLive(
        shardMap.get(shardName).getHost(), shardMap.get(shardName).getPort())) {
      return 0;
    }
    final String currentTimeSecondsStr = String.valueOf(System.currentTimeMillis() / 1000.0);
    return RedisUtils.executeWithConnection(
        shardMap.get(shardName).getGeneralRedisPool(),
        new Function<Jedis, Integer>() {
          @Override
          public Integer apply(Jedis conn) {
            int totalCount = 0;
            for (int priority : priorities) {
              String queueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                  queueName, shardName, priority, jobState);
              String hashRedisKeyPrefix = RedisBackendUtils.constructHashRedisKeyPrefix(
                  queueName, shardName);
              long count;

              // If a body matching regex was defined in the request, we need to use a LUA script
              // to run through and check each job's body. If not, a simple ZCOUNT can be used.
              if (bodyRegexToMatch == null) {
                if (countFutureJobs) {
                  count = conn.zcount(queueRedisKey, currentTimeSecondsStr, "+inf");
                } else {
                  count = conn.zcount(queueRedisKey, "-inf", String.valueOf(currentTimeSecondsStr));
                }
              } else {
                List<String> keys = Lists.newArrayList(queueRedisKey, hashRedisKeyPrefix);
                List<String> argv = countFutureJobs
                                    ? Lists.newArrayList(currentTimeSecondsStr, "+inf",
                    bodyRegexToMatch) :
                                    Lists.newArrayList("-inf", currentTimeSecondsStr,
                                        bodyRegexToMatch);
                count = (Long) conn.eval(RedisLuaScripts.COUNT_JOBS_MATCH_BODY, keys, argv);
              }
              totalCount += count;
            }
            return totalCount;
          }
        });
  }

  @Override
  protected String enqueueSingleJob(final String queueName, final PinLaterJob job,
                                    int numAutoRetries) throws Exception {
    final double currentTimeSeconds = System.currentTimeMillis() / 1000.0;

    // Check whether the queue to enqueue exists.
    if ((queueNames.get() == null) || !queueNames.get().contains(queueName)) {
      reloadQueueNames();
      if (!queueNames.get().contains(queueName)) {
        Stats.incr("redis-queue-not-found-enqueue");
        throw new PinLaterException(ErrorCode.QUEUE_NOT_FOUND, "Queue not found: " + queueName);
      }
    }
    final ImmutableMap.Entry<String, RedisPools> shard = getRandomEnqueueableShard();
    if (shard == null) {
      throw new PinLaterException(ErrorCode.NO_HEALTHY_SHARDS, "Unable to find healthy shard");
    }
    try {
      return RedisUtils.executeWithConnection(
          shard.getValue().getGeneralRedisPool(),
          new Function<Jedis, String>() {
            @Override
            public String apply(Jedis conn) {
              String jobIdRedisKey = RedisBackendUtils.constructJobIdRedisKey(
                  queueName, shard.getKey());
              String hashRedisKeyPrefix = RedisBackendUtils.constructHashRedisKeyPrefix(
                  queueName, shard.getKey());
              String queueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                  queueName, shard.getKey(), job.getPriority(), PinLaterJobState.PENDING);
              List<String> keys = Lists.newArrayList(
                  jobIdRedisKey, hashRedisKeyPrefix, queueRedisKey);
              double jobToRunTimestampSeconds;
              if (job.getRunAfterTimestampMillis() != 0) {
                jobToRunTimestampSeconds = job.getRunAfterTimestampMillis() / 1000.0;
              } else {
                jobToRunTimestampSeconds = currentTimeSeconds;
              }
              List<String> argv = Lists.newArrayList(
                  BytesUtil.stringFromByteBuffer(ByteBuffer.wrap(job.getBody())),
                  String.valueOf(job.getNumAttemptsAllowed()),
                  String.valueOf(currentTimeSeconds),
                  String.valueOf(jobToRunTimestampSeconds),
                  RedisBackendUtils.truncateCustomStatus(job.getCustomStatus())
              );
              Long jobId = (Long) conn.eval(RedisLuaScripts.ENQUEUE_JOB, keys, argv);
              return new PinLaterJobDescriptor(
                  queueName, shard.getKey(), job.getPriority(), jobId).toString();
            }
          });
    } catch (JedisConnectionException e) {
      if (numAutoRetries > 0) {
        // Retry the enqueue, potentially on a different shard.
        Stats.incr("enqueue-failures-retry");
        return enqueueSingleJob(queueName, job, numAutoRetries - 1);
      }
      String host = shard.getValue().getHost();
      Stats.incr("shard_connection_failed_" + host);
      LOG.error("Failed to get a redis connection.", e);
      throw new PinLaterException(ErrorCode.SHARD_CONNECTION_FAILED,
          String.format("Redis connection to %s failed", host));
    }
  }

  @Override
  protected PinLaterDequeueResponse dequeueJobsFromShard(
      final String queueName,
      final String shardName,
      final int priority,
      final String claimDescriptor,
      final int jobsNeeded,
      final int numAutoRetries,
      final boolean dryRun) throws Exception {
    // Skip the shard if it is unhealthy.
    if (!healthChecker.isServerLive(
        shardMap.get(shardName).getHost(), shardMap.get(shardName).getPort())) {
      return null;
    }
    final double currentTimeSeconds = System.currentTimeMillis() / 1000.0;
    try {
      return RedisUtils.executeWithConnection(
          shardMap.get(shardName).getGeneralRedisPool(),
          new Function<Jedis, PinLaterDequeueResponse>() {
            @Override
            public PinLaterDequeueResponse apply(Jedis conn) {
              PinLaterDequeueResponse shardResponse = new PinLaterDequeueResponse();
              final long currentTimeMillis = System.currentTimeMillis();

              String pendingQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                  queueName, shardName, priority, PinLaterJobState.PENDING);
              String inProgressQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                  queueName, shardName, priority, PinLaterJobState.IN_PROGRESS);

              if (dryRun) {
                // If this is a dry run, just retrieve the relevant pending jobs' local ids and
                // include their respective bodies. No need to use LUA script.
                Set<String> jobIdStrs = conn.zrangeByScore(
                    pendingQueueRedisKey, "-inf", String.valueOf(currentTimeSeconds), 0,
                    jobsNeeded);
                for (String jobIdStr : jobIdStrs) {
                  PinLaterJobDescriptor jobDesc = new PinLaterJobDescriptor(
                      queueName, shardName, priority, Long.parseLong(jobIdStr));
                  String hashRedisKey = RedisBackendUtils.constructHashRedisKey(
                      queueName, shardName, Long.parseLong(jobIdStr));
                  // When dry run, we may dequeue invalid tasks to the client.
                  String bodyStr = conn.hget(
                      hashRedisKey, RedisBackendUtils.PINLATER_JOB_HASH_BODY_FIELD);
                  ByteBuffer body = BytesUtil.stringToByteBuffer(bodyStr);
                  int attemptsAllowed = RedisBackendUtils.parseJobHashAttemptsAllowed(conn.hget(
                      hashRedisKey, RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_ALLOWED_FIELD));
                  int attemptsRemaining = RedisBackendUtils.parseJobHashAttemptsRemaining(conn.hget(
                      hashRedisKey, RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD));
                  shardResponse.putToJobs(jobDesc.toString(), body);
                  PinLaterDequeueMetadata metadata = new PinLaterDequeueMetadata();
                  metadata.setAttemptsAllowed(attemptsAllowed);
                  metadata.setAttemptsRemaining(attemptsRemaining);
                  shardResponse.putToJobMetadata(jobDesc.toString(), metadata);
                }
              } else {
                // If not a dry run, then we'll want to actually move the jobs from pending queue to
                // in progress queue. These two operations are done in transaction to guarantee that
                // no job is lost.
                List<String> keys = Lists.newArrayList(
                    pendingQueueRedisKey,
                    inProgressQueueRedisKey,
                    RedisBackendUtils.constructHashRedisKeyPrefix(queueName, shardName));
                List<String> argv = Lists.newArrayList(
                    String.valueOf(currentTimeSeconds),
                    String.valueOf(jobsNeeded),
                    claimDescriptor);
                Object dequeuedJobs = conn.eval(RedisLuaScripts.DEQUEUE_JOBS, keys, argv);
                List<Object> objects = (List<Object>) dequeuedJobs;
                for (int i = 0; i < objects.size(); i += 6) {
                  long jobId = Long.parseLong((String) objects.get(i));
                  PinLaterJobDescriptor jobDesc = new PinLaterJobDescriptor(
                      queueName, shardName, priority, jobId);
                  shardResponse.putToJobs(jobDesc.toString(),
                      BytesUtil.stringToByteBuffer((String) objects.get(i + 1)));
                  PinLaterDequeueMetadata metadata = new PinLaterDequeueMetadata();
                  int attemptsAllowed = RedisBackendUtils.parseJobHashAttemptsAllowed(
                      (String) objects.get(i + 2));
                  int attemptsRemaining = RedisBackendUtils.parseJobHashAttemptsRemaining(
                      (String) objects.get(i + 3));
                  metadata.setAttemptsAllowed(attemptsAllowed);
                  metadata.setAttemptsRemaining(attemptsRemaining);
                  shardResponse.putToJobMetadata(jobDesc.toString(), metadata);
                  long createAtMillis = RedisBackendUtils.parseJobHashCreatedAt(
                      (String) objects.get(i + 4));
                  long updateAtMillis = RedisBackendUtils.parseJobHashUpdatedAt(
                      (String) objects.get(i + 5));
                  if (attemptsAllowed == attemptsRemaining) {
                    Stats.addMetric(String.format("%s_first_dequeue_delay_ms", queueName),
                        (int) (currentTimeMillis - createAtMillis));
                  }
                  Stats.addMetric(String.format("%s_dequeue_delay_ms", queueName),
                      (int) (currentTimeMillis - updateAtMillis));
                }
              }
              return shardResponse;
            }
          });
    } catch (JedisConnectionException e) {
      if (numAutoRetries > 0) {
        // Retry on the same shard.
        Stats.incr("dequeue-failures-retry");
        return dequeueJobsFromShard(queueName, shardName, priority, claimDescriptor,
            jobsNeeded, numAutoRetries - 1, dryRun);
      }
      String host = shardMap.get(shardName).getHost();
      Stats.incr("shard_connection_failed_" + host);
      LOG.error("Failed to get a redis connection.", e);
      throw new PinLaterException(ErrorCode.SHARD_CONNECTION_FAILED,
          String.format("Redis connection to %s failed", host));
    }
  }

  @Override
  protected void ackSingleJob(final String queueName,
                              final boolean succeeded,
                              final PinLaterJobAckInfo jobAckInfo,
                              int numAutoRetries) throws Exception {
    final double currentTimeSeconds = System.currentTimeMillis() / 1000.0;
    final PinLaterJobDescriptor jobDesc = new PinLaterJobDescriptor(jobAckInfo.getJobDescriptor());
    try {
      RedisUtils.executeWithConnection(
          shardMap.get(jobDesc.getShardName()).getGeneralRedisPool(),
          new Function<Jedis, Void>() {
            @Override
            public Void apply(Jedis conn) {
              String pendingQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                  queueName, jobDesc.getShardName(), jobDesc.getPriority(),
                  PinLaterJobState.PENDING);
              String inProgressQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                  queueName, jobDesc.getShardName(), jobDesc.getPriority(),
                  PinLaterJobState.IN_PROGRESS);
              String succeededQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                  queueName, jobDesc.getShardName(), jobDesc.getPriority(),
                  PinLaterJobState.SUCCEEDED);
              String failedQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                  queueName, jobDesc.getShardName(), jobDesc.getPriority(),
                  PinLaterJobState.FAILED);
              String hashRedisKeyPrefix = RedisBackendUtils.constructHashRedisKeyPrefix(
                  queueName, jobDesc.getShardName());

              // The return of the ACK script might return -1 to indicate the acked job is not in
              // the
              // in progress queue. We do not handle it right now(e.g. throw an error),
              // because there
              // are cases where client thinks the ack is lost and resends the ack.
              if (succeeded) {
                // Handle succeeded job: move it to succeeded queue and set custom status.
                List<String> keys = Lists.newArrayList(
                    inProgressQueueRedisKey, hashRedisKeyPrefix, succeededQueueRedisKey);
                List<String> argv = Lists.newArrayList(
                    String.valueOf(jobDesc.getLocalId()),
                    String.valueOf(currentTimeSeconds),
                    RedisBackendUtils.truncateCustomStatus(jobAckInfo.getAppendCustomStatus()));
                conn.eval(RedisLuaScripts.ACK_SUCCEEDED_JOB, keys, argv);
              } else {
                // Handle failed job. Depending on whether the job has attempts remaining, we need
                // to either move it to pending or failed queue, and set custom status either way.
                // This logic is handled in the LUA script.
                List<String> keys = Lists.newArrayList(
                    inProgressQueueRedisKey, hashRedisKeyPrefix, pendingQueueRedisKey,
                    failedQueueRedisKey);
                List<String> argv = Lists.newArrayList(
                    String.valueOf(jobDesc.getLocalId()),
                    String.valueOf(currentTimeSeconds),
                    RedisBackendUtils.truncateCustomStatus(jobAckInfo.getAppendCustomStatus()),
                    String.valueOf(currentTimeSeconds + jobAckInfo.getRetryDelayMillis() / 1000.0));
                conn.eval(RedisLuaScripts.ACK_FAILED_JOB, keys, argv);
              }
              return null;
            }
          });
    } catch (JedisConnectionException e) {
      if (numAutoRetries > 0) {
        // Retry the ack.
        Stats.incr("ack-failures-retry");
        ackSingleJob(queueName, succeeded, jobAckInfo, numAutoRetries - 1);
        return;
      }
      String host = shardMap.get(jobDesc.getShardName()).getHost();
      Stats.incr("shard_connection_failed_" + host);
      LOG.error("Failed to get a redis connection.", e);
      throw new PinLaterException(ErrorCode.SHARD_CONNECTION_FAILED,
          String.format("Redis connection to %s failed", host));
    }
  }

  @Override
  protected void checkpointSingleJob(final String source,
                                     final String queueName,
                                     final PinLaterCheckpointJobRequest request,
                                     int numAutoRetries) throws Exception {
    final double currentTimeSeconds = System.currentTimeMillis() / 1000.0;
    final PinLaterJobDescriptor jobDesc = new PinLaterJobDescriptor(request.getJobDescriptor());
    try {
      RedisUtils.executeWithConnection(
          shardMap.get(jobDesc.getShardName()).getGeneralRedisPool(),
          new Function<Jedis, Void>() {
            @Override
            public Void apply(Jedis conn) {
              String pendingQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(queueName,
                  jobDesc.getShardName(), jobDesc.getPriority(), PinLaterJobState.PENDING);
              String inProgressQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(queueName,
                  jobDesc.getShardName(), jobDesc.getPriority(), PinLaterJobState.IN_PROGRESS);
              String hashRedisKeyPrefix = RedisBackendUtils.constructHashRedisKeyPrefix(
                  queueName, jobDesc.getShardName());

              List<String> keys = Lists.newArrayList(
                  inProgressQueueRedisKey,
                  request.isMoveToPending() ? pendingQueueRedisKey : inProgressQueueRedisKey,
                  hashRedisKeyPrefix);
              List<String> argv = Lists.newArrayList(
                  String.valueOf(jobDesc.getLocalId()),
                  RedisBackendUtils.escapeLuaMagicCharacters(source),
                  String.valueOf(request.isSetRunAfterTimestampMillis()
                                 ? request.getRunAfterTimestampMillis() / 1000.0 :
                                 currentTimeSeconds),
                  request.isSetNewBody() ? request.getNewBody() : "",
                  String.valueOf(request.isSetNumOfAttemptsAllowed()
                                 ? request.getNumOfAttemptsAllowed() : 0),
                  request.isSetPrependCustomStatus() ? request.getPrependCustomStatus() : ""
              );
              StringBuilder scriptBuilder =
                  new StringBuilder(RedisLuaScripts.CHECKPOINT_JOB_HEADER);

              if (request.isSetNewBody()) {
                scriptBuilder.append(RedisLuaScripts.CHECKPOINT_JOB_NEW_BODY);
              }
              if (request.isSetNumOfAttemptsAllowed()) {
                scriptBuilder.append(RedisLuaScripts.CHECKPOINT_JOB_NEW_ATTEMPTS_ALLOWED);
              }
              if (request.isSetPrependCustomStatus()) {
                scriptBuilder.append(RedisLuaScripts.CHECKPOINT_JOB_NEW_CUSTOM_STATUS);
              }
              if (request.isMoveToPending()) {
                scriptBuilder.append(RedisLuaScripts.CHECKPOINT_JOB_RESET_CLAIM_DESCRIPTOR);
              }

              scriptBuilder.append(RedisLuaScripts.CHECKPOINT_JOB_FOOTER);

              long numJobsAffected =
                  (Long) conn.eval(scriptBuilder.toString(), keys, argv);

              // If the number of jobs affected was 0 then the checkpoint request must have treated
              // as a no-op, in which case we should log and record the discrepancy. Note that
              // this can happen if: 1) the job is not found to be in the expected state (in
              // progress), or 2) the job's claim descriptor does not agree with the source that
              // made the checkpoint request.
              if (numJobsAffected == 0) {
                LOG.info("Checkpoint request was treated as a no-op from source: {}. Request: {}",
                    source, request);
                Stats.incr(queueName + "_checkpoint_noop");
              }

              return null;
            }
          });
    } catch (JedisConnectionException e) {
      if (numAutoRetries > 0) {
        // Retry the checkpoint.
        Stats.incr("checkpoint-failures-retry");
        checkpointSingleJob(source, queueName, request, numAutoRetries - 1);
        return;
      }
      String host = shardMap.get(jobDesc.getShardName()).getHost();
      Stats.incr("shard_connection_failed_" + host);
      LOG.error("Failed to get a redis connection.", e);
      throw new PinLaterException(ErrorCode.SHARD_CONNECTION_FAILED,
          String.format("Redis connection to %s failed", host));
    }
  }

  @Override
  protected Set<String> getQueueNamesImpl() throws Exception {
    if (shardMap.isEmpty()) {
      return Sets.newHashSet();
    }
    reloadQueueNames();
    return queueNames.get();
  }

  @Override
  protected List<PinLaterJobInfo> scanJobsFromShard(
      final String queueName,
      final String shardName,
      final Set<Integer> priorities,
      final PinLaterJobState jobState,
      final boolean scanFutureJobs,
      final String continuation,
      final int limit,
      final String bodyRegexToMatch) throws Exception {
    // Skip the shard if it is unhealthy.
    if (!healthChecker.isServerLive(
        shardMap.get(shardName).getHost(), shardMap.get(shardName).getPort())) {
      return Lists.newArrayListWithCapacity(0);
    }
    final double currentTimeSeconds = System.currentTimeMillis() / 1000.0;
    final String minScore = scanFutureJobs ? String.valueOf(currentTimeSeconds) : "-inf";
    final String maxScore = scanFutureJobs ? "+inf" : String.valueOf(currentTimeSeconds);
    return RedisUtils.executeWithConnection(
        shardMap.get(shardName).getGeneralRedisPool(),
        new Function<Jedis, List<PinLaterJobInfo>>() {
          @Override
          public List<PinLaterJobInfo> apply(Jedis conn) {
            List<List<PinLaterJobInfo>> jobsPerPriority =
                Lists.newArrayListWithCapacity(priorities.size());
            for (final int priority : priorities) {
              if (bodyRegexToMatch == null) {
                // If we don't need to match the job bodies with a regex, then we can just use
                // pipelining to scan the jobs.
                String queueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                    queueName, shardName, priority, jobState);
                // Get the job ids and their timestamps when they go into the queue.
                Set<Tuple> jobIdScoreTuples = conn.zrevrangeByScoreWithScores(
                    queueRedisKey, maxScore, minScore, 0, limit);

                // Get the jobs' detailed information in pipeline.
                HashMap<String, Response<Map<String, String>>> jobIdToDetails = Maps.newHashMap();
                Pipeline pipeline = conn.pipelined();
                for (Tuple tuple : jobIdScoreTuples) {
                  String jobIdStr = tuple.getElement();
                  jobIdToDetails.put(jobIdStr, pipeline.hgetAll(String.format(
                      "%s%s",
                      RedisBackendUtils.constructHashRedisKeyPrefix(queueName, shardName),
                      jobIdStr)));
                }
                pipeline.sync();

                // Create PinLaterJobInfo object for each job and add to jobsPerPriority.
                // We use the score of the job in the queue sorted set as the
                // runAfterTimestampMillis.
                List<PinLaterJobInfo> jobs = Lists.newArrayList();
                for (Tuple tuple : jobIdScoreTuples) {
                  String jobIdStr = tuple.getElement();
                  Map<String, String> jobDetails = jobIdToDetails.get(jobIdStr).get();
                  // If find the job body is empty, probably the job hash has been evicted. Do not
                  // return the invalid tasks to the client.
                  if (jobDetails.containsKey(RedisBackendUtils.PINLATER_JOB_HASH_BODY_FIELD)) {
                    PinLaterJobInfo jobInfo = new PinLaterJobInfo(
                        new PinLaterJobDescriptor(
                            queueName, shardName, priority, Long.valueOf(jobIdStr)).toString(),
                        jobState,
                        RedisBackendUtils.parseJobHashAttemptsAllowed(jobDetails.get(
                            RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_ALLOWED_FIELD)),
                        RedisBackendUtils.parseJobHashAttemptsRemaining(jobDetails.get(
                            RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD)),
                        RedisBackendUtils.parseJobHashCustomStatus(jobDetails.get(
                            RedisBackendUtils.PINLATER_JOB_HASH_CUSTOM_STATUS_FIELD)),
                        RedisBackendUtils.parseJobHashCreatedAt(jobDetails.get(
                            RedisBackendUtils.PINLATER_JOB_HASH_CREATED_AT_FIELD)));
                    jobInfo.setRunAfterTimestampMillis((long) (tuple.getScore() * 1000));
                    jobInfo.setUpdatedAtTimestampMillis(RedisBackendUtils.parseJobHashUpdatedAt(
                        jobDetails.get(RedisBackendUtils.PINLATER_JOB_HASH_UPDATED_AT_FIELD)));
                    jobInfo.setClaimDescriptor(
                        jobDetails.get(RedisBackendUtils.PINLATER_JOB_HASH_CLAIM_DESCRIPTOR_FIELD));
                    jobs.add(jobInfo);
                  } else {
                    Stats.incr(String.format(
                        RedisBackendUtils.REDIS_JOB_HASH_NOT_FOUND_STATS_FORMAT,
                        queueName, shardName, priority, "scan"));
                  }
                }
                jobsPerPriority.add(jobs);
              } else {
                // If we do need to match bodies with a regex then we should use a LUA script so 
                // we can match job bodies on the actual redis boxes in order to minimize network
                // IO.
                String queueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                    queueName, shardName, priority, jobState);
                String hashRedisKeyPrefix = RedisBackendUtils.constructHashRedisKeyPrefix(
                    queueName, shardName);
                List<String> keys = Lists.newArrayList(queueRedisKey, hashRedisKeyPrefix);
                List<String> argv = Lists.newArrayList(
                    String.valueOf(limit), minScore, maxScore, bodyRegexToMatch);
                List<Object> results =
                    (List<Object>) conn.eval(RedisLuaScripts.SCAN_JOBS_MATCH_BODY, keys, argv);

                // Create jobs list and add to jobsPerPriority list.
                List<PinLaterJobInfo> jobs = Lists.newArrayList();
                for (int i = 0; i < results.size(); i += 8) {
                  PinLaterJobInfo jobInfo = new PinLaterJobInfo(
                      new PinLaterJobDescriptor(queueName, shardName, priority,
                          Long.parseLong((String) results.get(i))).toString(),
                      jobState,
                      RedisBackendUtils.parseJobHashAttemptsAllowed((String) results.get(i + 1)),
                      RedisBackendUtils.parseJobHashAttemptsRemaining((String) results.get(i + 2)),
                      RedisBackendUtils.parseJobHashCustomStatus((String) results.get(i + 3)),
                      RedisBackendUtils.parseJobHashCreatedAt((String) results.get(i + 4)));
                  jobInfo.setUpdatedAtTimestampMillis(
                      RedisBackendUtils.parseJobHashUpdatedAt((String) results.get(i + 5)));
                  jobInfo.setClaimDescriptor((String) results.get(i + 6));
                  jobInfo.setRunAfterTimestampMillis(
                      (long) (Double.parseDouble((String) results.get(i + 7)) * 1000));
                  jobs.add(jobInfo);
                }
                jobsPerPriority.add(jobs);
              }
            }
            return PinLaterBackendUtils.mergeIntoList(
                jobsPerPriority, PinLaterBackendUtils.JobInfoComparator.getInstance());
          }
        }
    );
  }

  @Override
  protected int retryFailedJobsFromShard(
      final String queueName,
      final String shardName,
      final int priority,
      final int attemptsRemaining,
      final long runAfterTimestampMillis,
      final int limit) throws Exception {
    // Skip the shard if it is unhealthy.
    if (!healthChecker.isServerLive(
        shardMap.get(shardName).getHost(), shardMap.get(shardName).getPort())) {
      return 0;
    }
    return RedisUtils.executeWithConnection(
        shardMap.get(shardName).getGeneralRedisPool(),
        new Function<Jedis, Integer>() {
          @Override
          public Integer apply(Jedis conn) {
            String failedQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                queueName, shardName, priority, PinLaterJobState.FAILED);
            String pendingQueueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                queueName, shardName, priority, PinLaterJobState.PENDING);
            String hashRedisKeyPrefix = RedisBackendUtils.constructHashRedisKeyPrefix(
                queueName, shardName);
            List<String> keys = Lists.newArrayList(
                failedQueueRedisKey, pendingQueueRedisKey, hashRedisKeyPrefix);
            List<String> argv = Lists.newArrayList(
                String.valueOf(runAfterTimestampMillis / 1000.0),
                String.valueOf(limit),
                String.valueOf(attemptsRemaining));
            Object result = conn.eval(RedisLuaScripts.RETRY_JOBS, keys, argv);
            return ((Long) result).intValue();
          }
        });
  }

  @Override
  protected int deleteJobsFromShard(
      final String queueName,
      final String shardName,
      final PinLaterJobState jobState,
      final int priority,
      final String bodyRegexToMatch,
      final int limit) throws Exception {
    // Skip the shard if it is unhealthy.
    if (!healthChecker.isServerLive(
        shardMap.get(shardName).getHost(), shardMap.get(shardName).getPort())) {
      return 0;
    }
    return RedisUtils.executeWithConnection(
        shardMap.get(shardName).getGeneralRedisPool(),
        new Function<Jedis, Integer>() {
          @Override
          public Integer apply(Jedis conn) {
            Object result;
            String queueRedisKey = RedisBackendUtils.constructQueueRedisKey(
                queueName, shardName, priority, jobState);
            String hashRedisKeyPrefix = RedisBackendUtils.constructHashRedisKeyPrefix(
                queueName, shardName);
            List<String> keys = Lists.newArrayList(queueRedisKey, hashRedisKeyPrefix);
            if (bodyRegexToMatch == null) {
              List<String> argv = Lists.newArrayList(String.valueOf(limit));
              result = conn.eval(RedisLuaScripts.DELETE_JOBS, keys, argv);
            } else {
              List<String> argv = Lists.newArrayList(String.valueOf(limit), bodyRegexToMatch);
              result = conn.eval(RedisLuaScripts.DELETE_JOBS_MATCH_BODY, keys, argv);
            }
            return ((Long) result).intValue();
          }
        });
  }

  /**
   * Clean up all the keys in each shard. This method is only for test use.
   */
  @VisibleForTesting
  public Future<Void> cleanUpAllShards() {
    return futurePool.apply(new ExceptionalFunction0<Void>() {
      @Override
      public Void applyE() throws Throwable {
        for (final ImmutableMap.Entry<String, RedisPools> shard : shardMap.entrySet()) {
          RedisUtils.executeWithConnection(
              shard.getValue().getGeneralRedisPool(),
              new Function<Jedis, Void>() {
                @Override
                public Void apply(Jedis conn) {
                  conn.flushAll();
                  return null;
                }
              });
        }
        return null;
      }
    });
  }

  @VisibleForTesting
  ImmutableMap<String, RedisPools> getEnqueueableShards() {
    ImmutableMap.Builder<String, RedisPools> redisPoolShardMapBuilder =
        new ImmutableMap.Builder<String, RedisPools>();

    for (ImmutableMap.Entry<String, RedisPools> shard : shardMap.entrySet()) {
      if (!shard.getValue().getDequeueOnly()) {
        redisPoolShardMapBuilder.put(shard.getKey(), shard.getValue());
      }
    }

    return redisPoolShardMapBuilder.build();
  }

  private ImmutableMap.Entry<String, RedisPools> getRandomEnqueueableShard() {
    ImmutableMap<String, RedisPools> enqueueableShardMap = getEnqueueableShards();
    return getRandomShard(enqueueableShardMap, healthChecker, RANDOM, true);
  }

  /**
   * Get a random shard from shardMap.
   *
   * @param healthyOnly if true only returns random shard from healthy shards.
   * @return a random shard from shardMap or from healthy shards of shardMap if healthyOnly is set.
   */
  private Map.Entry<String, RedisPools> getRandomShard(final boolean healthyOnly) {
    return getRandomShard(shardMap, healthChecker, RANDOM, healthyOnly);
  }

  @VisibleForTesting
  public static Map.Entry<String, RedisPools> getRandomShard(
      final ImmutableMap<String, RedisPools> shardMap,
      final HealthChecker healthChecker,
      final Random random,
      final boolean healthyOnly) {
    Map<String, RedisPools> filteredShardMap;
    if (healthyOnly) {
      filteredShardMap = Maps.filterValues(shardMap, new Predicate<RedisPools>() {
        @Override
        public boolean apply(@Nullable RedisPools redisPools) {
          return healthChecker.isServerLive(redisPools.getHost(), redisPools.getPort());
        }
      });
      if (filteredShardMap.size() == 0) {
        return null;
      }
    } else {
      filteredShardMap = shardMap;
    }
    return (Map.Entry) filteredShardMap.entrySet().toArray()[
        random.nextInt(filteredShardMap.size())];
  }

  /**
   * Reload queue names from redis to local cache.
   */
  private synchronized void reloadQueueNames() throws Exception {
    ImmutableSet.Builder<String> builder = new ImmutableSet.Builder<String>();
    if (!shardMap.isEmpty()) {
      final Map.Entry<String, RedisPools> randomShard = getRandomShard(true);
      if (randomShard == null) {
        throw new PinLaterException(ErrorCode.NO_HEALTHY_SHARDS, "Unable to find healthy shard");
      }
      Set<String> newQueueNames = RedisUtils.executeWithConnection(
          randomShard.getValue().getGeneralRedisPool(),
          new Function<Jedis, Set<String>>() {
            @Override
            public Set<String> apply(Jedis conn) {
              return RedisBackendUtils.getQueueNames(conn, randomShard.getKey());
            }
          });
      builder.addAll(newQueueNames);
    }
    queueNames.set(builder.build());
  }

  /**
   * Remove the job hash from redis. This function is used in test to simulate the case where the
   * job id is still in the queue, while the job hash is evicted by redis LRU.
   */
  @VisibleForTesting
  public Future<Void> removeJobHash(String jobDescriptor) {
    final PinLaterJobDescriptor jobDesc = new PinLaterJobDescriptor(jobDescriptor);
    return futurePool.apply(new ExceptionalFunction0<Void>() {
      @Override
      public Void applyE() throws Throwable {
        RedisUtils.executeWithConnection(
            shardMap.get(jobDesc.getShardName()).getGeneralRedisPool(),
            new Function<Jedis, Void>() {
              @Override
              public Void apply(Jedis conn) {
                String hashRedisKey = RedisBackendUtils.constructHashRedisKey(
                    jobDesc.getQueueName(), jobDesc.getShardName(), jobDesc.getLocalId());
                conn.del(hashRedisKey);
                return null;
              }
            });
        return null;
      }
    });
  }
}
