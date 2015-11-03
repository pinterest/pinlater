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

/**
 * API definition for PinLater, a service for deferred job execution.
 */

namespace py thrift_libs
namespace java com.pinterest.pinlater.thrift
namespace cpp pinterest.pinlater

enum ErrorCode {
  // Client errors: 400 - 499
  CONTINUATION_INVALID = 400,
  PASSWORD_INVALID = 403,
  QUEUE_NAME_INVALID = 410,
  QUEUE_NAME_TOO_LONG = 411,
  QUEUE_NOT_FOUND = 413,
  PRIORITY_NOT_SUPPORTED = 416,
  DEQUEUE_RATE_LIMITED = 429,
  ENQUEUE_REJECTED = 430,

  // Server errors: 500 - 599
  UNKNOWN = 500,
  NO_HEALTHY_SHARDS = 501,
  SHARD_CONNECTION_FAILED = 502,
}

/**
 * States that PinLater jobs can be in. Jobs are inserted in the PENDING state,
 * which means they are eligible for dequeue (subject to run_after timestamp constraint).
 * When dequeued, they go into IN_PROGRESS. When ACK'ed:
 *   if the execution was successful, they go into SUCCEEDED.
 *   if the execution failed and execution attempts are exhausted, they go into FAILED.
 *   if the execution failed but execution attempts are available, they go back into PENDING.
 *
 * Jobs in IN_PROGRESS state that are not ACK'ed within some time become eligible for
 * resurrection back to PENDING.
 *
 * Jobs in SUCCEEDED or FAILED state will be GC'ed after a period of time.
 *
 */
enum PinLaterJobState {
  PENDING = 0,
  IN_PROGRESS = 1,
  SUCCEEDED = 2,
  FAILED = 3,
}

const string CONTINUATION_START = "0"
const string CONTINUATION_END = "-1"

/**
 * Individual PinLater job. The only required field is the actual body of the job (which is an
 * uninterpreted byte array), but the following config parameters can optionally be specified:
 *
 * priority: Job priority, currently in the range 1-3 (1 is highest). Default is 1.
 *
 * runAfterTimestampMillis: Minimum time at which this job should run (milliseconds since epoch).
 * If not, specified, the job is eligible to run immediately.
 *
 * numAttemptsAllowed: Maximum number of attempts (including the first attempt and retries) to make
 * to execute this job, in the event of failures. Default is 2, i.e. one retry.
 *
 * customStatus: An arbitrary string to track custom status for the job. Custom status truncation
 * behavior is backend specific. This is not interpreted by PinLater in any way, and is only meant
 * for manual job tracking and debugging.
 */
struct PinLaterJob {
  1: required binary body;
  2: optional byte priority = 1;
  3: optional i64 runAfterTimestampMillis;
  4: optional i32 numAttemptsAllowed = 2;
  5: optional string customStatus = "";
}

struct PinLaterEnqueueRequest {
  1: required string queueName;
  2: required list<PinLaterJob> jobs;
}

struct PinLaterEnqueueResponse {
  1: required list<string> jobDescriptors;
}

/**
 * Identifies the job to ACK. The jobDescriptor is required. 'appendCustomStatus' if
 * provided will be appended to the custom status provided for the job at enqueue time. However,
 * backend may choose to store only the most recent custom status.
 * Note that the custom status cannot exceed certain characters, which is also backend specific.
 * For failure acks, an optional retry delay duration can be specified, which will alter the
 * run_after timestamp to be equal to the current timestamp plus the retry delay. This option is
 * ignored for successful acks or jobs which have exceeded their allowed attempts.
 */
struct PinLaterJobAckInfo {
  1: required string jobDescriptor;
  2: optional string appendCustomStatus = "";
  3: optional i64 retryDelayMillis = 0;
}

struct PinLaterJobAckRequest {
  1: required string queueName;
  2: optional list<PinLaterJobAckInfo> jobsSucceeded;
  3: optional list<PinLaterJobAckInfo> jobsFailed;
}

struct PinLaterDequeueRequest {
  1: required string queueName;
  2: required i32 limit;
  3: optional PinLaterJobAckRequest jobAckRequest;
  4: optional bool dryRun = 0;
  5: optional double coverage = 1.0;
}

struct PinLaterDequeueMetadata {
  1: optional i32 attemptsAllowed;
  2: optional i32 attemptsRemaining;
}

/**
 * Contains an optional map "jobs" which holds mappings between job descriptor and job bodies.
 * Also contains an optional map "jobMetadata" which holds additional information about those
 * dequeued jobs (# attempts allowed, # attempts remaining, etc.)
 */
struct PinLaterDequeueResponse {
  1: optional map<string, binary> jobs;
  2: optional map<string, PinLaterDequeueMetadata> jobMetadata;
}

struct PinLaterCheckpointJobRequest {
  1: required string jobDescriptor;
  2: optional string newBody;
  3: optional bool moveToPending = 0;
  4: optional i64 runAfterTimestampMillis;
  5: optional i32 numOfAttemptsAllowed;
  6: optional string prependCustomStatus;
}

struct PinLaterCheckpointJobsRequest {
  1: required string queueName;
  2: required list<PinLaterCheckpointJobRequest> requests;
}

struct PinLaterLookupJobRequest {
  1: required list<string> jobDescriptors;
  2: optional bool includeBody = 0;
}

struct PinLaterJobInfo {
  1:  required string jobDescriptor;
  2:  required PinLaterJobState jobState;
  10: required i32 attemptsAllowed;
  3:  required i32 attemptsRemaining;
  4:  required string customStatus;
  5:  required i64 createdAtTimestampMillis;
  6:  optional i64 runAfterTimestampMillis;
  7:  optional i64 updatedAtTimestampMillis;
  8:  optional string claimDescriptor;
  9:  optional binary body;
}

/**
 * Request to count the number of jobs in a given queue of a particular state. By default, jobs
 * of all priorities will be counted but an optional priority field gives the option to just count
 * jobs of a particular priority. Also, by default, only jobs scheduled to run now are counted. An
 * optional parameter can be toggled to have the count query count future jobs instead. A string
 * can also be specified so that only jobs with bodies matching that regex string will be counted.
 */
struct PinLaterGetJobCountRequest {
  1: required string queueName;
  2: required PinLaterJobState jobState;
  3: optional byte priority;
  4: optional bool countFutureJobs = 0;
  5: optional string bodyRegexToMatch;
}

/**
 * Request to scan for a batch of jobs (and associated job info except for body), sorted by
 * descending run_after timestamp. If job bodies are desired, please use lookupJobs() on the
 * returned job IDs. By default, jobs of all priorities will be scanned but an optional
 * priority field gives the option to just scan jobs of a particular priority. Also, by
 * default, only jobs scheduled to run now will be returned. An optional parameter can be
 * toggled to have the scan query return future jobs instead. Continuation + pagination are not
 * supported at the moment and must be the default value CONTINUATION_START. A optional string
 * can also be specified so that only jobs with bodies matching that regex string will be scanned.
 */
struct PinLaterScanJobsRequest {
  1: required string queueName;
  2: required PinLaterJobState jobState;
  3: optional byte priority;
  4: optional bool scanFutureJobs = 0;
  5: optional string continuation = CONTINUATION_START;
  6: optional i32 limit = 100;
  7: optional string bodyRegexToMatch;
}

struct PinLaterScanJobsResponse {
  1: required list<PinLaterJobInfo> scannedJobs;
  2: optional string continuation = CONTINUATION_END;
}

struct PinLaterRetryFailedJobsRequest {
  1: required string queueName;
  2: optional byte priority = 1;
  3: optional i32 attemptsToAllow = 1;
  4: optional i64 runAfterTimestampMillis;
  5: optional i32 limit = 1000;
}

struct PinLaterDeleteJobsRequest {
  1: required string queueName;
  2: required PinLaterJobState jobState;
  3: optional byte priority = 1;
  4: optional string bodyRegexToMatch;
  5: optional i32 limit = 1000;
}

exception PinLaterException {
  1: required ErrorCode errorCode;
  2: required string message;
}

/**
 * Context associated with each PinLater request, typically used for logging and metrics.
 * Source is a required string identifying the caller, typically set to "servicename:hostname".
 * RequestId is an optional string id associated with a given request.
 */
struct RequestContext {
  1: required string source;
  2: optional string requestId = "";
}

service PinLater {
  /**
   * Creates a new queue with the given name. Queues must be created before jobs can be enqueued
   * or dequeued from them. If the queue already exists on a particular shard, the operation will
   * act as a a no-op. This method is idempotent and safe to retry.
   *
   * Queue names can only contain alphanumeric characters and underscores.
   */
  void createQueue(1: RequestContext context, 2: string name) throws(1: PinLaterException e);

  /**
   * Deletes a queue with the given name. If the queue does not exist on a particular shard, the
   * operation will act as a a no-op. This method is idempotent and safe to retry.
   */
  void deleteQueue(1: RequestContext context, 2: string name, 3: string password)
      throws(1: PinLaterException e);

  /**
   * Enqueues a list of jobs into a specified PinLater queue. The queue must have been previously
   * created, otherwise an exception will be returned. If successful, this will return unique job
   * descriptors (string ids) for the enqueued jobs. These are useful for debugging/tracing.
   */
  PinLaterEnqueueResponse enqueueJobs(1: RequestContext context, 2: PinLaterEnqueueRequest request)
      throws(1: PinLaterException e);

  /**
   * Dequeues a set of jobs from a specified PinLater queue, upto the specified limit. The queue
   * must have been previously created, otherwise an exception will be returned. Jobs eligible
   * for execution will be dequeued in priority order.
   *
   * An optional coverage can also be specified. This is an optimization where a client can instruct
   * the server to potentially examine only a subset of the shards in a partitioned storage backend
   * before returning a response. Such an optimization could help in the scenario where there are
   * potentially empty queues being queried. Caveat: this can give an imprecise answer, so only
   * meant for advanced use. Most users should keep it at 1.0.
   *
   * This API can optionally be used to also ACK previously dequeued jobs, so that workers doing
   * a sequence of dequeue/ack need not make separate RPCs for the two steps. There is also the
   * option to declare the call a dry run, which retrieves available jobs without running them.
   */
  PinLaterDequeueResponse dequeueJobs(1: RequestContext context, 2: PinLaterDequeueRequest request)
      throws(1: PinLaterException e);

  /**
   * ACK previously dequeued jobs for a specific queue. Jobs can be marked succeeded or failed.
   * Failed jobs will automatically be retried upto the number of attempts the job was configured
   * with.
   *
   * Workers doing a sequence of dequeue/ack should specify jobAcks as a payload in the dequeue
   * request rather than calling this API separately.
   */
  void ackDequeuedJobs(1: RequestContext context, 2: PinLaterJobAckRequest request)
      throws(1: PinLaterException e);

  /**
   * Notify the PinLater service that the jobs specified are actively being worked on. This is
   * useful in the following situations: 1) if a job takes a long time and the user doesn't want
   * the server to think that the job has timed out, 2) implementing cron-like jobs, 3)
   * implementing chained jobs (e.g. jobs that by nature enqueue a version of themselves after
   * they finish).
   *
   * An optional param can be specified to replace the job's body with a new string, which is
   * particularly useful in the case of implementing chained jobs. Also, the job can optionally
   * be specified to be put back into the pending state to be dequeued. In this particular case,
   * a runAfter timestamp may be specified, which provides the ability to schedule a cron-like job.
   */
  void checkpointJobs(1: RequestContext context, 2: PinLaterCheckpointJobsRequest request)
       throws(1: PinLaterException e);

  /**
   * Lookup jobs by job descriptor without actually dequeuing them. This will by default
   * only return job state and custom status, but can optionally return the entire job body.
   * If a job was not found, it will not be included in the returned map.
   */
  map<string, PinLaterJobInfo> lookupJobs(1: RequestContext context,
                                          2: PinLaterLookupJobRequest request)
      throws(1: PinLaterException e);

  /**
   * Lookup the number of jobs in a queue of a certain state. By default the method counts jobs
   * of all priorities that are scheduled to run now, but the request also has optional parameters
   * to count jobs of a particular priority or ones that are scheduled to run in the future. The
   * count returned is currently capped at 100k * (num of shards) for the MySQL implementation of
   * PinLater.
   */
  i32 getJobCount(1: RequestContext context, 2: PinLaterGetJobCountRequest request)
      throws(1: PinLaterException e);

  /**
   * Get the names of all existing queues.
   */
  set<string> getQueueNames(1: RequestContext context) throws(1: PinLaterException e);

  /**
   * Grab a batch of jobs and relevant job information, sorted by most recent first based on the
   * runAfter timestamp. The ordering is currently unstable, since it is possible for two run_after
   * timestamps to be identical. Also, continuation and pagination are currently not supported.
   */
  PinLaterScanJobsResponse scanJobs(1: RequestContext context, 2: PinLaterScanJobsRequest request)
      throws(1: PinLaterException e);

  /**
   * Retry failed jobs in a specified queue and return the number of jobs that were granted retries.
   * The priority of jobs to be retried can be specified and will default to 1. The number of
   * retries granted is also configurable (default 1). There is an optional field that can be set
   * if a particular time for the jobs to be run after is desired. The number of jobs to retry is
   * 1000 by default, but can be configured to another value in order to avoid putting stress on
   * the backend all at once when retrying many jobs.
   */
  i32 retryFailedJobs(1: RequestContext context, 2: PinLaterRetryFailedJobsRequest request)
      throws(1: PinLaterException e);

  /**
   * Delete jobs in a queue that match the specified conditions and return the number of jobs that
   * were actually deleted. One optional condition that can be specified is the priority of
   * jobs (which will default to 1). There is also an optional string that can be set that will
   * have the delete operation delete only jobs with bodies that match that regex string. The
   * limit on the number of jobs that can be deleted is 1000 by default, but can be configured to
   * another value in order to avoid putting stress on the backend all at once when deleting many
   * jobs.
   */
  i32 deleteJobs(1: RequestContext context, 2: PinLaterDeleteJobsRequest request)
      throws(1: PinLaterException e);
}
