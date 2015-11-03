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

/**
 * Encapsulates Redis LUA scripts used by the PinLaterRedisBackend and related classes.
 */
public final class RedisLuaScripts {

  private RedisLuaScripts() {}

  /*
   * Enqueue job with given job information.
   *
   * Increment and get the jobId for the new job. Form the hash key for this job and store the
   * given job information into the hash.
   *
   * Args:
   *     KEYS[1]: Auto incremental jobId string key.
   *     KEYS[2]: Hash key prefix.
   *     KEYS[3]: Priority queue sorted set key.
   *     ARGV[1]: Job body.
   *     ARGV[2]: Remaining(Allowed) attempts.
   *     ARGV[3]: Created at timestamp in seconds(float).
   *     ARGV[4]: Job to run timestamp in seconds(float).
   *     ARGV[5]: Custom status.
   *
   * Returns:
   *     The job Id of the enqueued job.
   */
  public static final String ENQUEUE_JOB =
      "local jobId = redis.call('INCR', KEYS[1])\n"
          + "redis.call('HMSET', KEYS[2]..jobId,"
          + " '" + RedisBackendUtils.PINLATER_JOB_HASH_BODY_FIELD + "', ARGV[1],"
          + " '" + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_ALLOWED_FIELD + "', ARGV[2],"
          + " '" + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD + "', ARGV[2],"
          + " '" + RedisBackendUtils.PINLATER_JOB_HASH_CREATED_AT_FIELD + "', ARGV[3],"
          + " '" + RedisBackendUtils.PINLATER_JOB_HASH_UPDATED_AT_FIELD + "', ARGV[3],"
          + " '" + RedisBackendUtils.PINLATER_JOB_HASH_CUSTOM_STATUS_FIELD + "', ARGV[5])"
          + "redis.call('ZADD', KEYS[3], ARGV[4], jobId)\n"
          + "return jobId";

  /*
   * Dequeue limit number of jobs in the queue up to the given timestamp.
   *
   * Get ``limit`` number of jobs in the given sorted set up to the given timestamp. The returned
   * number of jobs might be smaller than ``limit`` if we do not have that many jobs in that queue.
   * Remove the number of returned jobs from the sorted set, which ensures that we remove the
   * obtained job ids. Add them with the new timestamp to the in progress queue. For each job, if
   * the job hash does not exist, probably the job hash has been evicted. Do not dequeue invalid
   * jobs to the client. Otherwise, get the job detailed information and return them altogether in
   * a list.
   *
   * Args:
   *     KEYS[1]: Pending queue sorted set key.
   *     KEYS[2]: In progress queue sorted set key.
   *     KEYS[3]: Hash key prefix.
   *     ARGV[1]: Current timestamp.
   *     ARGV[2]: Limit.
   *     ARGV[3]: Claim descriptor.
   *
   * Returns:
   *     A list of 6n objects, where every 6 objects represents the job ID, body, attempts allowed,
   *     attempts remaining, created at, and updated at. Note it is unfortunate we have to return
   *     the data in this format since LUA 'dict' and list of 'list' is not convertible to JAVA
   *     or Jedis.
   */
  public static final String DEQUEUE_JOBS =
      "local jobIds = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', '0',"
          + " ARGV[2])\n"
          + "local length = table.getn(jobIds)\n"
          + "local result = {}\n"
          + "if length > 0 then\n"
          + "redis.call('ZREMRANGEBYRANK', KEYS[1], 0, length - 1)\n"
          + "local zadd_args = {}\n"
          + "for i, jobId in ipairs(jobIds) do\n"
          + "table.insert(zadd_args, ARGV[1])\n"
          + "table.insert(zadd_args, jobId)\n"
          + "if redis.call('EXISTS', KEYS[3]..jobId) == 1 then\n"
          + "local jobInfo = redis.call('HMGET', KEYS[3]..jobId, '"
          + RedisBackendUtils.PINLATER_JOB_HASH_BODY_FIELD + "', '"
          + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_ALLOWED_FIELD + "', '"
          + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD + "', '"
          + RedisBackendUtils.PINLATER_JOB_HASH_CREATED_AT_FIELD + "', '"
          + RedisBackendUtils.PINLATER_JOB_HASH_UPDATED_AT_FIELD + "')\n"
          + "table.insert(result, jobId)\n"
          + "table.insert(result, jobInfo[1])\n"
          + "table.insert(result, jobInfo[2])\n"
          + "table.insert(result, jobInfo[3])\n"
          + "table.insert(result, jobInfo[4])\n"
          + "table.insert(result, jobInfo[5])\n"
          + "redis.call('HMSET', KEYS[3]..jobId, '"
          + RedisBackendUtils.PINLATER_JOB_HASH_UPDATED_AT_FIELD + "', ARGV[1], '"
          + RedisBackendUtils.PINLATER_JOB_HASH_CLAIM_DESCRIPTOR_FIELD + "', ARGV[3])\n"
          + "end\n"
          + "end\n"
          + "redis.call('ZADD', KEYS[2], unpack(zadd_args))\n"
          + "end\n"
          + "return result";

  /*
   * Remove the job from the in progress queue. This script should only be used to be prepended to
   * ACK_SUCCEEDED_JOB and ACK_FAILED_JOB scripts.
   *
   * If this job is not in the in progress queue, then the script will just return -1. All other
   * operations appended to this script will not be executed.
   *
   * Args:
   *     KEYS[1]: In progress queue sorted set key.
   *     ARGV[1]: Job id.
   */
  private static final String REMOVE_JOB_FROM_IN_PROGRESS_QUEUE =
      "local num = redis.call('ZREM', KEYS[1], ARGV[1])\n"
          + "if num ~= 1 then\n"
          + "return -1\n"
          + "end\n";

  /*
   * Set the custom status and update the updated timestamp. This script should only be used to be
   * appended to ACK_SUCCEEDED_JOB and ACK_FAILED_JOB scripts.
   *
   * If the to append custom status is not empty, get the previous custom status. If the previous
   * one is empty, just set the new custom status. Otherwise, append the new one.
   *
   * Args:
   *     KEYS[2]: Hash key prefix.
   *     ARGV[1]: Job id.
   *     ARGV[2]: Current timestamp.
   *     ARGV[3]: To append custom status.
   */
  private static final String SET_CUSTOM_STATUS =
      "redis.call('HMSET', KEYS[2]..ARGV[1],"
          + " '" + RedisBackendUtils.PINLATER_JOB_HASH_CUSTOM_STATUS_FIELD + "', ARGV[3],"
          + " '" + RedisBackendUtils.PINLATER_JOB_HASH_UPDATED_AT_FIELD + "', ARGV[2])\n";

  /*
   * Acknowledge the succeed job.
   *
   * Remove the job from the in progress queue and add to the succeeded queue. Also append the
   * custom status.
   *
   * Args:
   *     KEYS[1]: In progress queue sorted set key.
   *     KEYS[2]: Hash key prefix.
   *     KEYS[3]: Succeeded queue sorted set key.
   *     ARGV[1]: Job id.
   *     ARGV[2]: Current timestamp.
   *     ARGV[3]: Custom status.
   */
  public static final String ACK_SUCCEEDED_JOB =
      REMOVE_JOB_FROM_IN_PROGRESS_QUEUE
          + "redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])\n"
          + SET_CUSTOM_STATUS;

  /*
   * Acknowledge the failed job.
   *
   * Remove the job from the in progress queue. Check the remaining attempts of the job. If it is
   * > 1, add it to pending queue with the given timestamp. Otherwise, add it to the failed queue.
   * Finally append the custom status.
   *
   * Args:
   *     KEYS[1]: In progress queue sorted set key.
   *     KEYS[2]: Hash key prefix.
   *     KEYS[3]: Pending queue sorted set key.
   *     KEYS[4]: Failed queue sorted set key.
   *     ARGV[1]: Job id.
   *     ARGV[2]: Current timestamp in seconds(float).
   *     ARGV[3]: Custom status.
   *     ARGV[4]: Retry timestamp in seconds(float).
   */
  public static final String ACK_FAILED_JOB =
      REMOVE_JOB_FROM_IN_PROGRESS_QUEUE
          + "local att = tonumber(redis.call('HGET', KEYS[2]..ARGV[1], '"
          + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD + "'))\n"
          + "if att == nil then\n" +
          // Job hash has been evicted. GC will take care of it so ignore.
          "return\n"
          + "elseif att > 1 then\n"
          + "redis.call('ZADD', KEYS[3], ARGV[4], ARGV[1])\n"
          + "redis.call('HDEL', KEYS[2]..ARGV[1], '"
          + RedisBackendUtils.PINLATER_JOB_HASH_CLAIM_DESCRIPTOR_FIELD + "')\n"
          + "else\n"
          + "redis.call('ZADD', KEYS[4], ARGV[2], ARGV[1])\n"
          + "end\n"
          + "redis.call('HINCRBY', KEYS[2]..ARGV[1], '"
          + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD + "', -1)\n"
          + SET_CUSTOM_STATUS;

  /*
   * Handle the timeout jobs in in-progress queue.
   *
   * Get the timeouted jobs from the in progress queue. Check the remaining attempts of the job.
   * If it does not have, probably the hash of this job has been evicted. Just
   * it is >1, add it to pending queue with the given timestamp. Otherwise, add it to the failed
   * queue. Finally, decrement the remaining attempts of the job.
   *
   * Args:
   *     KEYS[1]: In progress queue sorted set key.
   *     KEYS[2]: Hash key prefix.
   *     KEYS[3]: Pending queue sorted set key.
   *     KEYS[4]: Failed queue sorted set key.
   *     ARGV[1]: Timeout timestamp.
   *     ARGV[2]: The max number of jobs to update.
   *     ARGV[3]: Current timestamp.
   *
   * Returns:
   *     A length of 2 list. The first element is the number of jobs that are moved to failed queue.
   *     The second element is the number of jobs that are moved to pending queue.
   */
  public static final String MONITOR_TIMEOUT_UPDATE =
      "local jobIds = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', '0',"
          + " ARGV[2])\n"
          + "local length = table.getn(jobIds)\n"
          + "local doneNum = 0\n"
          + "local retryNum = 0\n"
          + "local evictNum = 0\n"
          + "if length > 0 then\n"
          + "redis.call('ZREMRANGEBYRANK', KEYS[1], 0, length - 1)\n"
          + "for i, jobId in ipairs(jobIds) do\n"
          + "local att = tonumber(redis.call('HGET', KEYS[2]..jobId, '"
          + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD + "'))\n"
          + "if att == nil then\n"
          + "evictNum = evictNum + 1\n"
          + "else\n"
          + "if att > 1 then\n"
          + "retryNum = retryNum + 1\n"
          + "redis.call('ZADD', KEYS[3], ARGV[3], jobId)\n"
          + "redis.call('HDEL', KEYS[2]..jobId, '"
          + RedisBackendUtils.PINLATER_JOB_HASH_CLAIM_DESCRIPTOR_FIELD + "')\n"
          + "else\n"
          + "doneNum = doneNum + 1\n"
          + "redis.call('ZADD', KEYS[4], ARGV[3], jobId)\n"
          + "end\n"
          + "redis.call('HINCRBY', KEYS[2]..jobId, '"
          + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD + "', -1)\n"
          + "redis.call('HSET', KEYS[2]..jobId, '"
          + RedisBackendUtils.PINLATER_JOB_HASH_UPDATED_AT_FIELD + "', ARGV[3])\n"
          + "end\n"
          + "end\n"
          + "end\n"
          + "return {tostring(doneNum), tostring(retryNum), tostring(evictNum)}";

  /*
   * Garbage collect done jobs.
   *
   * Get and remove up to limit number of expired jobs from the given queue. Also remove the hash
   * key for each of these jobs.
   *
   * Args:
   *     KEYS[1]: Succeeded queue sorted set key.
   *     KEYS[2]: Hash key prefix.
   *     ARGV[1]: Expired timestamp.
   *     ARGV[2]: The max number of jobs to update.
   *
   * Returns:
   *     Number of jobs that have been removed.
   */
  public static final String MONITOR_GC_DONE_JOBS =
      "local jobIds = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', '0',"
          + " ARGV[2])\n"
          + "local length = table.getn(jobIds)\n"
          + "if length > 0 then\n"
          + "redis.call('ZREMRANGEBYRANK', KEYS[1], 0, length - 1)\n"
          + "for i, jobId in ipairs(jobIds) do\n"
          + "redis.call('DEL', KEYS[2]..jobId)\n"
          + "end\n"
          + "end\n"
          + "return length";

  /*
   * Delete one sorted set of the queue.
   *
   * Args:
   *     KEYS[1]: Queue sorted set key.
   *     KEYS[2]: Hash key prefix.
   *
   * Returns:
   *     Number of jobs that have been removed.
   */
  public static final String DELETE_QUEUE =
      "local jobIds = redis.call('ZRANGE', KEYS[1], 0, -1)\n"
          + "redis.call('DEL', KEYS[1])\n"
          + "for i, jobId in ipairs(jobIds) do\n"
          + "redis.call('DEL', KEYS[2]..jobId)\n"
          + "end\n"
          + "return table.getn(jobIds)";

  /*
   * Move jobs from failed queue to pending queue.
   *
   * Get up to ``limit`` number of jobs from the failed queue and put them to the pending queue.
   * For each job, also update the attempts remaining, updated_at timestamp, and reset the claim
   * descriptor.
   *
   * Args:
   *     KEYS[1]: Failed queue sorted set key.
   *     KEYS[2]: Pending queue sorted set key.
   *     KEYS[3]: Hash key prefix.
   *     ARGV[1]: Current timestamp in seconds(float).
   *     ARGV[2]: Limit, as the number of jobs to move.
   *     ARGV[3]: Remaining attempts to set.
   *
   * Returns:
   *     Number of jobs that have been moved.
   */
  public static final String RETRY_JOBS =
      "local jobIds = redis.call('ZRANGE', KEYS[1], 0, ARGV[2] - 1)\n"
          + "local length = table.getn(jobIds)\n"
          + "if length > 0 then\n"
          + "redis.call('ZREMRANGEBYRANK', KEYS[1], 0, length - 1)\n"
          + "local zadd_args = {}\n"
          + "for i, jobId in ipairs(jobIds) do\n"
          + "table.insert(zadd_args, ARGV[1])\n"
          + "table.insert(zadd_args, jobId)\n"
          + "redis.call('HMSET', KEYS[3]..jobId, '"
          + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD + "', ARGV[3], '"
          + RedisBackendUtils.PINLATER_JOB_HASH_UPDATED_AT_FIELD + "', ARGV[1])\n"
          + "redis.call('HDEL', KEYS[3]..jobId, '"
          + RedisBackendUtils.PINLATER_JOB_HASH_CLAIM_DESCRIPTOR_FIELD + "')\n"
          + "end\n"
          + "redis.call('ZADD', KEYS[2], unpack(zadd_args))\n"
          + "end\n"
          + "return length";

  /*
   * Delete jobs of a specified queue and state.
   *
   * Remove up to ``limit`` number of jobs from the specified queue (which returns job IDs) and
   * then call delete on each individual job ID key to get rid of their associated data stored in
   * job hash (e.g. body, custom status, etc.).
   *
   * Args:
   *     KEYS[1]: Queue (associated with a particular state) sorted set key.
   *     KEYS[2]: Hash key prefix.
   *     ARGV[1]: Limit, as the number of jobs to move.
   *
   * Returns:
   *     Number of jobs that have been deleted.
   */
  public static final String DELETE_JOBS =
      "local jobIds = redis.call('ZRANGE', KEYS[1], 0, ARGV[1] - 1)\n"
          + "local length = table.getn(jobIds)\n"
          + "if length > 0 then\n"
          + "    redis.call('ZREMRANGEBYRANK', KEYS[1], 0, length - 1)\n"
          + "    for i, jobId in ipairs(jobIds) do\n"
          + "        redis.call('DEL', KEYS[2]..jobId)\n"
          + "    end\n"
          + "end\n"
          + "return length";

  /*
   * Delete jobs that match the specified regex string in the specified queue and state.
   *
   * Check up to ``limit`` number of jobs from the specified queue (which returns job IDs) and
   * their respective bodies. If the job body matches the specified regex string, then call delete
   * on each individual job ID key to get rid of their associated data stored in job hash (e.g.
   * body, custom status, etc.).
   *
   * Args:
   *     KEYS[1]: Queue (associated with a particular state) sorted set key.
   *     KEYS[2]: Hash key prefix.
   *     ARGV[1]: Limit, as the number of jobs to move.
   *     ARGV[2]: Regex string to match body with.
   *
   * Returns:
   *     Number of jobs that have been deleted.
   */
  public static final String DELETE_JOBS_MATCH_BODY =
      "local limit = tonumber(ARGV[1])\n"
          + "local start = 0\n"
          + "local numDeleted = 0\n"
          + "local moreToFetch = true\n"
          + "while moreToFetch do\n"
          + "    local jobIds = redis.call('ZRANGE', KEYS[1], start, start + ARGV[1] - 1)\n"
          + "    for i, jobId in ipairs(jobIds) do\n"
          + "        local body = redis.call('HGET', KEYS[2]..jobId, '"
          + RedisBackendUtils.PINLATER_JOB_HASH_BODY_FIELD + "')\n"
          + "        if string.match(body, ARGV[2]) then\n"
          + "            redis.call('ZREM', KEYS[1], jobId)\n"
          + "            redis.call('DEL', KEYS[2]..jobId)\n"
          + "            numDeleted = numDeleted + 1\n"
          + "            if numDeleted >= limit then\n"
          + "                return numDeleted\n"
          + "            end\n"
          + "        end\n"
          + "    end\n"
          + "    local numFetched = table.getn(jobIds)\n"
          + "    start = start + numFetched\n"
          + "    moreToFetch = numFetched == limit\n"
          + "end\n"
          + "return numDeleted";

  /*
   * Count the number of jobs with bodies that match the regex string.
   *
   * Go through all jobs in the specified queue and counts the number of jobs that have
   * bodies matching the regex string.
   *
   * Args:
   *     KEYS[1]: Queue (associated with a particular state) sorted set key.
   *     KEYS[2]: Hash key prefix.
   *     ARGV[1]: Lower bound score to scan the sorted set with.
   *     ARGV[2]: Upper bound score to scan the sorted set with.
   *     ARGV[3]: Regex string to match body with.
   *
   * Returns:
   *     Number of matching jobs.
   */
  public static final String COUNT_JOBS_MATCH_BODY =
      "local count = 0\n"
          + "local jobIds = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2])\n"
          + "for i, jobId in ipairs(jobIds) do\n"
          + "    local body = redis.call('HGET', KEYS[2]..jobId, '"
          + RedisBackendUtils.PINLATER_JOB_HASH_BODY_FIELD + "')\n"
          + "    if string.match(body, ARGV[3]) then\n"
          + "        count = count + 1\n"
          + "    end\n"
          + "end\n"
          + "return count";

  /*
   * Scan jobs with bodies that match the regex string.
   *
   * Go through all jobs in the specified queue and finds jobs that have bodies matching the regex
   * string.
   *
   * Args:
   *     KEYS[1]: Queue (associated with a particular state) sorted set key.
   *     KEYS[2]: Hash key prefix.
   *     ARGV[1]: Limit for number of jobs to scan.
   *     ARGV[2]: Lower bound score to scan the sorted set with.
   *     ARGV[3]: Upper bound score to scan the sorted set with.
   *     ARGV[4]: Regex string to match body with.
   *
   * Returns:
   *     A list of 8n objects, where every 8 objects represents the job ID, attempts allowed,
   *     attempts remaining, custom status, created at, updated at, claim descriptor,
   *     and run after. Note it is unfortunate we have to return the data in this format since
   *     LUA 'dict' and list of 'list' is not convertible to JAVA or Jedis.
   */
  public static final String SCAN_JOBS_MATCH_BODY =
      "local result = {}\n"
          + "local limit = tonumber(ARGV[1])\n"
          + "local start = 0\n"
          + "local numScanned = 0\n"
          + "local moreToFetch = true\n"
          + "while moreToFetch do\n"
          + "    local jobIdsAndScores = redis.call('ZREVRANGEBYSCORE', KEYS[1], ARGV[3], ARGV[2],"
          + "                                       'WITHSCORES', 'LIMIT', start, limit)\n"
          + "    local jobIdsAndScoresLength = table.getn(jobIdsAndScores)\n"
          + "    for i=1,jobIdsAndScoresLength,2 do\n"
          + "        local jobId = jobIdsAndScores[i]\n"
          + "        local score = jobIdsAndScores[i + 1]\n"
          + "        local body = redis.call('HGET', KEYS[2]..jobId, '"
          + RedisBackendUtils.PINLATER_JOB_HASH_BODY_FIELD + "')\n"
          + "        if string.match(body, ARGV[4]) then\n"
          + "            local jobInfo = redis.call('HMGET', KEYS[2]..jobId, '"
          + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_ALLOWED_FIELD + "', '"
          + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD + "', '"
          + RedisBackendUtils.PINLATER_JOB_HASH_CUSTOM_STATUS_FIELD + "', '"
          + RedisBackendUtils.PINLATER_JOB_HASH_CREATED_AT_FIELD + "', '"
          + RedisBackendUtils.PINLATER_JOB_HASH_UPDATED_AT_FIELD + "', '"
          + RedisBackendUtils.PINLATER_JOB_HASH_CLAIM_DESCRIPTOR_FIELD + "')\n"
          + "            table.insert(result, jobId)\n"
          + "            table.insert(result, jobInfo[1])\n"
          + "            table.insert(result, jobInfo[2])\n"
          + "            table.insert(result, jobInfo[3])\n"
          + "            table.insert(result, jobInfo[4])\n"
          + "            table.insert(result, jobInfo[5])\n"
          + "            table.insert(result, jobInfo[6])\n"
          + "            table.insert(result, score)\n"
          + "            numScanned = numScanned + 1\n"
          + "            if numScanned >= limit then\n"
          + "                return result\n"
          + "            end\n"
          + "        end\n"
          + "    end\n"
          + "    local numFetched = jobIdsAndScoresLength / 2\n"
          + "    start = start + numFetched\n"
          + "    moreToFetch = numFetched == limit\n"
          + "end\n"
          + "return result";

  /*
   * Common checkpointing logic that should be prepend to checkpoint request scripts.
   *
   * Args:
   *     KEYS[1]: Key of sorted set that the job currently lies in.
   *     KEYS[2]: Key of sorted set that the job is to be moved to.
   *     KEYS[3]: Hash key prefix.
   *     ARGV[1]: Job id.
   *     ARGV[2]: Source (hostname) of checkpoint request.
   *     ARGV[3]: Current timestamp in seconds (float).
   */
  public static final String CHECKPOINT_JOB_HEADER =
      "local claimDescriptor = redis.call('HGET', KEYS[3]..ARGV[1],"
          + " '" + RedisBackendUtils.PINLATER_JOB_HASH_CLAIM_DESCRIPTOR_FIELD + "')\n"
          + "if not claimDescriptor or not string.match(claimDescriptor, ARGV[2]) then\n"
          + "    return 0\n"
          + "end\n"
          + "redis.call('ZREM', KEYS[1], ARGV[1])\n"
          + "redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])\n"
          + "redis.call('HSET', KEYS[3]..ARGV[1], '"
          + RedisBackendUtils.PINLATER_JOB_HASH_UPDATED_AT_FIELD + "', ARGV[3])\n";

  /*
 * Update the run_after and updated_at fields of a job to the current time. Optionally
 * changes the job's state by moving it to a specified sorted set. Also optionally changes
 * the job body, resets the number of attempts allowed/remaining and prepend a message to the
 * custom status. A checkpoint request should be constructed as:
 *
 * CHECKPOINT_JOB_HEADER + [optional update fields] + CHECKPOINT_JOB_FOOTER
 *
 * Note that this update will only go through if the claim descriptor field matches the host
 * that made the checkpoint request. If it doesn't match, this script is a no-op.
 *
 * Args:
 *     KEYS[1]: Key of sorted set that the job currently lies in.
 *     KEYS[2]: Key of sorted set that the job is to be moved to.
 *     KEYS[3]: Hash key prefix.
 *     ARGV[1]: Job id.
 *     ARGV[2]: Source (hostname) of checkpoint request.
 *     ARGV[3]: Current timestamp in seconds (float)
 *     ARGV[4]: New job body.
 *     ARGV[5]: Set the number of attempts allowed (also reset the attempts remaining).
 *     ARGV[6]: Message to prepend to custom status.
 *
 * Returns:
 *     Number of jobs that have been affected by the checkpoint request. In other words, 0 if this
 *     operation was a no-op, and 1 if the checkpoint operation was successful.
 */
  public static final String CHECKPOINT_JOB_FOOTER =
      "return 1";

  public static final String CHECKPOINT_JOB_NEW_BODY =
      "redis.call('HSET', KEYS[3]..ARGV[1], '"
          + RedisBackendUtils.PINLATER_JOB_HASH_BODY_FIELD + "', ARGV[4])\n";

  public static final String CHECKPOINT_JOB_NEW_ATTEMPTS_ALLOWED =
      "redis.call('HMSET', KEYS[3]..ARGV[1], '"
          + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_ALLOWED_FIELD + "', ARGV[5], '"
          + RedisBackendUtils.PINLATER_JOB_HASH_ATTEMPTS_REMAINING_FIELD + "', ARGV[5])\n";

  public static final String CHECKPOINT_JOB_NEW_CUSTOM_STATUS =
      "local customStatus = string.sub(ARGV[6]..redis.call('HGET', KEYS[3]..ARGV[1],"
          + " '" + RedisBackendUtils.PINLATER_JOB_HASH_CUSTOM_STATUS_FIELD + "'),"
          + " 0, " + String.valueOf(RedisBackendUtils.CUSTOM_STATUS_SIZE_BYTES) + ")\n"
          + "redis.call('HSET', KEYS[3]..ARGV[1], '"
          + RedisBackendUtils.PINLATER_JOB_HASH_CUSTOM_STATUS_FIELD + "', customStatus)\n";

  public static final String CHECKPOINT_JOB_RESET_CLAIM_DESCRIPTOR =
      "redis.call('HDEL', KEYS[3]..ARGV[1], '"
          + RedisBackendUtils.PINLATER_JOB_HASH_CLAIM_DESCRIPTOR_FIELD + "')\n";
}
