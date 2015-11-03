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

import com.pinterest.pinlater.thrift.PinLaterJobState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * Encapsulates static utility methods used by the PinLaterRedisBackend and related classes.
 */
public final class RedisBackendUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RedisBackendUtils.class);

  public static final String PINLATER_QUEUE_KEY_PREFIX = "plq";
  public static final String PINLATER_JOB_ID_KEY_PREFIX = "plj";
  public static final String PINLATER_QUEUE_NAMES_KEY_PREFIX = "pln";
  public static final String PINLATER_JOB_HASH_KEY_PREFIX = "plh";
  public static final String PINLATER_JOB_HASH_BODY_FIELD = "bd";
  public static final String PINLATER_JOB_HASH_CUSTOM_STATUS_FIELD = "cs";
  public static final String PINLATER_JOB_HASH_CREATED_AT_FIELD = "ca";
  public static final String PINLATER_JOB_HASH_UPDATED_AT_FIELD = "ua";
  public static final String PINLATER_JOB_HASH_ATTEMPTS_ALLOWED_FIELD = "al";
  public static final String PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD = "ar";
  public static final String PINLATER_JOB_HASH_CLAIM_DESCRIPTOR_FIELD = "cd";

  // Default values to use when we cannot find from the job hash in redis.
  public static final String PINLATER_JOB_HASH_DEFAULT_BODY = "";
  public static final String PINLATER_JOB_HASH_DEFAULT_CUSTOM_STATUS = "";
  public static final long PINLATER_JOB_HASH_DEFAULT_CREATED_AT = 0L;
  public static final long PINLATER_JOB_HASH_DEFAULT_UPDATED_AT = 0L;
  public static final int PINLATER_JOB_HASH_DEFAULT_ATTEMPTS_ALLOWED = 0;
  public static final int PINLATER_JOB_HASH_DEFAULT_ATTEMPTS_REMAINING = 0;

  // Stats format used for incrementing when job hash not found.
  // The input should be (queueName, shardName, priority, operation).
  // e.g. "redis_job_hash_not_found_finishrepintasklowpri_1_1_dequeue".
  public static final String REDIS_JOB_HASH_NOT_FOUND_STATS_FORMAT =
      "redis_job_hash_not_found_%s_%s_%d_%s";

  @VisibleForTesting
  public static final int CUSTOM_STATUS_SIZE_BYTES = 3000;

  /**
   * Characters that considered "magic characters" in LUA.
   */
  public static final ImmutableSet<Character> LUA_MAGIC_CHARS = ImmutableSet.of(
      '%', '^', '$', '(', ')', '.', '[', ']', '*', '+', '-', '?');

  private RedisBackendUtils() {}

  /**
   * Constructs the redis queue sorted set key name given a queue name, shard id and priority.
   *
   * @param queueName Name of the queue.
   * @param shardName Redis shard name.
   * @param priority Priority level.
   * @param state Job state.
   * @return Redis sorted set key.
   */
  public static String constructQueueRedisKey(String queueName, String shardName, int priority,
                                              PinLaterJobState state) {
    return String.format("%s_%s_%s.p%1d_s%1d", PINLATER_QUEUE_KEY_PREFIX, shardName, queueName,
        priority, state.getValue());
  }

  /**
   * Constructs the redis hash key prefix for the given shard.
   *
   * The returned prefix is used to concatenate the jobId to form the full job hash key. This
   * function is used for LUA script, in which we concatenate the returned prefix with the jobId(s)
   * from redis.
   *
   * @param shardName Redis shard name.
   * @return Redis sorted set key.
   */
  public static String constructHashRedisKeyPrefix(String queueName, String shardName) {
    return String.format("%s_%s_%s.", PINLATER_JOB_HASH_KEY_PREFIX, shardName, queueName);
  }

  /**
   * Constructs the redis hash key for the given job inside the given shard.
   *
   * @param shardName Redis shard name.
   * @param jobId Local job id inside the shard.
   * @return Redis hash key.
   */
  public static String constructHashRedisKey(String queueName, String shardName, long jobId) {
    return String.format("%s%s", constructHashRedisKeyPrefix(queueName, shardName), jobId);
  }

  /**
   * Constructs the redis key used to increment and get the localId for jobs in the given queue.
   *
   * @param queueName Name of the queue.
   * @param shardName Redis shard name.
   * @return Redis string key.
   */
  public static String constructJobIdRedisKey(String queueName, String shardName) {
    return String.format("%s_%s_%s", PINLATER_JOB_ID_KEY_PREFIX, shardName, queueName);
  }

  /**
   * Constructs the redis key used to store the queue names sorted list in redis.
   *
   * @param shardName Redis shard name.
   * @return Redis string key.
   */
  public static String constructQueueNamesRedisKey(String shardName) {
    return String.format("%s_%s", PINLATER_QUEUE_NAMES_KEY_PREFIX, shardName);
  }

  /**
   * Creates the Redis shard map from config.
   *
   * @param redisConfigStream InputStream containing the Redis config json.
   * @param configuration PropertiesConfiguration object.
   * @return A map shardName -> RedisPools.
   */
  public static ImmutableMap<String, RedisPools> buildShardMap(
      InputStream redisConfigStream,
      PropertiesConfiguration configuration) {
    RedisConfigSchema redisConfig;
    try {
      redisConfig = RedisConfigSchema.read(Preconditions.checkNotNull(redisConfigStream));
    } catch (IOException e) {
      LOG.error("Failed to load redis configuration", e);
      throw new RuntimeException(e);
    }

    ImmutableMap.Builder<String, RedisPools> shardMapBuilder =
        new ImmutableMap.Builder<String, RedisPools>();
    for (RedisConfigSchema.Shard shard : redisConfig.shards) {
      shardMapBuilder.put(
          shard.name,
          new RedisPools(
              configuration,
              shard.shardConfig.master.host,
              shard.shardConfig.master.port,
              shard.shardConfig.dequeueOnly));
    }
    return shardMapBuilder.build();
  }

  /**
   * Truncate the given custom status under CUSTOM_STATUS_SIZE_BYTES bytes.
   *
   * @param customStatus A string.
   * @return A string, as the truncated custom status.
   */
  public static String truncateCustomStatus(String customStatus) {
    if (customStatus == null) {
      return customStatus;
    }
    return customStatus.length() > CUSTOM_STATUS_SIZE_BYTES
           ? customStatus.substring(0, CUSTOM_STATUS_SIZE_BYTES) : customStatus;
  }

  /**
   * Get all the queue names in the given shard. Note that it is the caller's responsibility to
   * close the connection after this method is called.
   *
   * @param conn Jedis connection for the given shard.
   * @param shardName The shard name for the given shard.
   * @return A set of strings, as all the queue names in that shard.
   */
  public static Set<String> getQueueNames(Jedis conn, String shardName) {
    String queueNamesRedisKey = RedisBackendUtils.constructQueueNamesRedisKey(shardName);
    // TODO(jiacheng): Consider to do pagination if we get many queues on one shard.
    return conn.zrange(queueNamesRedisKey, 0, -1);
  }

  /**
   * Functions to parse the values from redis hash. If it is null, returns the default value.
   */
  public static String parseJobHashBody(String s) {
    return s == null ? PINLATER_JOB_HASH_DEFAULT_BODY : s;
  }

  public static Integer parseJobHashAttemptsAllowed(String s) {
    return s == null ? PINLATER_JOB_HASH_DEFAULT_ATTEMPTS_ALLOWED : Integer.valueOf(s);
  }

  public static Integer parseJobHashAttemptsRemaining(String s) {
    return s == null ? PINLATER_JOB_HASH_DEFAULT_ATTEMPTS_REMAINING : Integer.valueOf(s);
  }

  public static String parseJobHashCustomStatus(String s) {
    return s == null ? PINLATER_JOB_HASH_DEFAULT_CUSTOM_STATUS : s;
  }

  public static Long parseJobHashCreatedAt(String s) {
    // Return milliseconds.
    return s == null ? PINLATER_JOB_HASH_DEFAULT_CREATED_AT : (long) (Double.valueOf(s) * 1000);
  }

  public static Long parseJobHashUpdatedAt(String s) {
    // Return milliseconds.
    return s == null ? PINLATER_JOB_HASH_DEFAULT_UPDATED_AT : (long) (Double.valueOf(s) * 1000);
  }

  /**
   * Returns a new string that escapes all LUA magic characters in the old string.
   *
   * @param oldStr Old string that we want to escape magic characters from.
   * @return New string with escaped magic characters.
   */
  public static String escapeLuaMagicCharacters(String oldStr) {
    StringBuffer newStr = new StringBuffer();
    for (int i = 0; i < oldStr.length(); i++) {
      char c = oldStr.charAt(i);
      // If this is a magic character, we want to escape it be prefixing it with a '%' char.
      if (LUA_MAGIC_CHARS.contains(c)) {
        newStr.append('%');
      }
      newStr.append(c);
    }
    return newStr.toString();
  }
}
