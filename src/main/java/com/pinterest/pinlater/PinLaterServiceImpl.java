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

import com.pinterest.pinlater.thrift.ErrorCode;
import com.pinterest.pinlater.thrift.PinLater;
import com.pinterest.pinlater.thrift.PinLaterCheckpointJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterDeleteJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterDequeueRequest;
import com.pinterest.pinlater.thrift.PinLaterDequeueResponse;
import com.pinterest.pinlater.thrift.PinLaterEnqueueRequest;
import com.pinterest.pinlater.thrift.PinLaterEnqueueResponse;
import com.pinterest.pinlater.thrift.PinLaterException;
import com.pinterest.pinlater.thrift.PinLaterGetJobCountRequest;
import com.pinterest.pinlater.thrift.PinLaterJobAckRequest;
import com.pinterest.pinlater.thrift.PinLaterJobInfo;
import com.pinterest.pinlater.thrift.PinLaterLookupJobRequest;
import com.pinterest.pinlater.thrift.PinLaterRetryFailedJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterScanJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterScanJobsResponse;
import com.pinterest.pinlater.thrift.RequestContext;

import com.google.common.base.Preconditions;
import com.twitter.common.base.MorePreconditions;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Function;
import com.twitter.util.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Class that implements the PinLater thrift API. Currently, this defers all the actual
 * work to the backend, but does logging and stats export.
 */
public class PinLaterServiceImpl implements PinLater.ServiceIface {

  private static final Logger LOG = LoggerFactory.getLogger(PinLaterServiceImpl.class);
  private static final Pattern VALID_QUEUE_NAME = Pattern.compile("\\w+");

  private final PinLaterBackendIface backend;
  private final PinLaterQueueConfig queueConfig;

  public PinLaterServiceImpl(PinLaterBackendIface backend, PinLaterQueueConfig queueConfig) {
    this.backend = Preconditions.checkNotNull(backend);
    this.queueConfig = Preconditions.checkNotNull(queueConfig);
  }

  @Override
  public Future<Void> createQueue(RequestContext context, final String name) {
    if (!validateQueueName(name)) {
      return Future.exception(new PinLaterException(
          ErrorCode.QUEUE_NAME_INVALID, "Invalid queue name: " + name));
    }
    return Stats.timeFutureMillis(
        "PinLaterService.createQueue",
        backend.createQueue(name).rescue(new LogAndWrapException<Void>(
            context, "createQueue", name)));
  }

  @Override
  public Future<Void> deleteQueue(
      RequestContext context, final String name, final String password) {
    return Stats.timeFutureMillis(
        "PinLaterService.deleteQueue",
        backend.deleteQueue(name, password).rescue(new LogAndWrapException<Void>(
            context, "deleteQueue", name)));
  }

  @Override
  public Future<PinLaterEnqueueResponse> enqueueJobs(
      RequestContext context, final PinLaterEnqueueRequest request) {
    return Stats.timeFutureMillis(
        "PinLaterService.enqueueJobs",
        backend.enqueueJobs(request).onSuccess(
            new Function<PinLaterEnqueueResponse, BoxedUnit>() {
              @Override
              public BoxedUnit apply(PinLaterEnqueueResponse response) {
                Stats.incr(request.getQueueName() + "_enqueue", request.getJobsSize());
                return null;
              }
            }).rescue(new LogAndWrapException<PinLaterEnqueueResponse>(
            context, "enqueueJobs", request.toString())));
  }

  @Override
  public Future<PinLaterDequeueResponse> dequeueJobs(
      RequestContext context, final PinLaterDequeueRequest request) {
    if (!queueConfig.allowDequeue(request.getQueueName(), request.getLimit())) {
      Stats.incr(request.getQueueName() + "_dequeue_requests_rate_limited");
      return Future.exception(new PinLaterException(ErrorCode.DEQUEUE_RATE_LIMITED,
          "Dequeue rate limit exceeded for queue: " + request.getQueueName()));
    }

    return Stats.timeFutureMillis(
        "PinLaterService.dequeueJobs",
        backend.dequeueJobs(context.getSource(), request).onSuccess(
            new Function<PinLaterDequeueResponse, BoxedUnit>() {
              @Override
              public BoxedUnit apply(PinLaterDequeueResponse response) {
                Stats.incr(request.getQueueName() + "_dequeue", response.getJobsSize());
                return null;
              }
            }).rescue(new LogAndWrapException<PinLaterDequeueResponse>(
            context, "dequeueJobs", request.toString())));
  }

  @Override
  public Future<Void> ackDequeuedJobs(RequestContext context, final PinLaterJobAckRequest request) {
    return Stats.timeFutureMillis(
        "PinLaterService.ackDequeuedJobs",
        backend.ackDequeuedJobs(request).onSuccess(
            new Function<Void, BoxedUnit>() {
              @Override
              public BoxedUnit apply(Void aVoid) {
                Stats.incr(request.getQueueName() + "_ack_succeeded",
                    request.getJobsSucceededSize());
                Stats.incr(request.getQueueName() + "_ack_failed", request.getJobsFailedSize());
                return null;
              }
            }).rescue(new LogAndWrapException<Void>(context, "ackDequeuedJobs",
            request.toString())));
  }

  @Override
  public Future<Void> checkpointJobs(RequestContext context,
                                     final PinLaterCheckpointJobsRequest request) {
    return Stats.timeFutureMillis(
        "PinLaterService.checkpointJobs",
        backend.checkpointJobs(context.getSource(), request).onSuccess(
            new Function<Void, BoxedUnit>() {
              @Override
              public BoxedUnit apply(Void aVoid) {
                Stats.incr(request.getQueueName() + "_checkpoint", request.getRequestsSize());
                return null;
              }
            }).rescue(new LogAndWrapException<Void>(
            context, "checkpointJobs", request.toString())));
  }

  @Override
  public Future<Map<String, PinLaterJobInfo>> lookupJobs(RequestContext context,
                                                         PinLaterLookupJobRequest request) {
    return Stats.timeFutureMillis(
        "PinLaterService.lookupJobs",
        backend.lookupJobs(request).rescue(
            new LogAndWrapException<Map<String, PinLaterJobInfo>>(
                context, "lookupJobs", request.toString())));
  }

  @Override
  public Future<Integer> getJobCount(RequestContext context, PinLaterGetJobCountRequest request) {
    return Stats.timeFutureMillis(
        "PinLaterService.getJobCount",
        backend.getJobCount(request).rescue(
            new LogAndWrapException<Integer>(context, "getJobCount", request.toString())));
  }

  @Override
  public Future<Set<String>> getQueueNames(RequestContext context) {
    return Stats.timeFutureMillis(
        "PinLaterService.getQueueNames",
        backend.getQueueNames().rescue(
            new LogAndWrapException<Set<String>>(context, "getQueueNames", "")));
  }

  @Override
  public Future<PinLaterScanJobsResponse> scanJobs(RequestContext context,
                                                   PinLaterScanJobsRequest request) {
    return Stats.timeFutureMillis(
        "PinLaterService.scanJobs",
        backend.scanJobs(request).rescue(
            new LogAndWrapException<PinLaterScanJobsResponse>(
                context, "scanJobs", request.toString())));
  }

  @Override
  public Future<Integer> retryFailedJobs(RequestContext context,
                                         PinLaterRetryFailedJobsRequest request) {
    return Stats.timeFutureMillis(
        "PinLaterService.retryFailedJobs",
        backend.retryFailedJobs(request).rescue(
            new LogAndWrapException<Integer>(
                context, "retryFailedJobs", request.toString())));
  }

  @Override
  public Future<Integer> deleteJobs(RequestContext context, PinLaterDeleteJobsRequest request) {
    return Stats.timeFutureMillis(
        "PinLaterService.deleteJobs",
        backend.deleteJobs(request).rescue(
            new LogAndWrapException<Integer>(
                context, "deleteJobs", request.toString())));
  }

  private boolean validateQueueName(String name) {
    return VALID_QUEUE_NAME.matcher(name).matches();
  }

  private static class LogAndWrapException<Response> extends Function<Throwable, Future<Response>> {

    private final RequestContext context;
    private final String methodName;
    private final String requestDesc;

    LogAndWrapException(RequestContext context, String methodName, String requestDesc) {
      this.context = Preconditions.checkNotNull(context);
      this.methodName = MorePreconditions.checkNotBlank(methodName);
      this.requestDesc = requestDesc;
      LOG.debug("Context: {} Method: {} Request: {}", context, methodName, requestDesc);
    }

    @Override
    public Future<Response> apply(Throwable throwable) {
      LOG.error("Context: {} Method: {} Request: {} Exception:",
          context, methodName, requestDesc, throwable);
      PinLaterException exception;
      if (throwable instanceof PinLaterException) {
        exception = (PinLaterException) throwable;
      } else {
        exception = new PinLaterException(ErrorCode.UNKNOWN, throwable.toString());
      }
      String errorStats = "PinLater." + methodName + ".errors."
          + errorCodeToStr(exception.getErrorCode());
      Stats.incr(errorStats);
      return Future.exception(exception);
    }
  }

  private static String errorCodeToStr(ErrorCode errorCode) {
    switch (errorCode) {
      case CONTINUATION_INVALID:
        return "CONTINUATION_INVALID";
      case PASSWORD_INVALID:
        return "PASSWORD_INVALID";
      case QUEUE_NAME_INVALID:
        return "QUEUE_NAME_INVALID";
      case QUEUE_NAME_TOO_LONG:
        return "QUEUE_NAME_TOO_LONG";
      case QUEUE_NOT_FOUND:
        return "QUEUE_NOT_FOUND";
      case DEQUEUE_RATE_LIMITED:
        return "DEQUEUE_RATE_LIMITED";
      case ENQUEUE_REJECTED:
        return "ENQUEUE_REJECTED";
      case PRIORITY_NOT_SUPPORTED:
        return "PRIORITY_NOT_SUPPORTED";
      case NO_HEALTHY_SHARDS:
        return "INTERNAL_ERROR";
      case SHARD_CONNECTION_FAILED:
        return "SHARD_CONNECTION_FAILED";
      case UNKNOWN:
        return "UNKNOWN";
      default:
        return "INVALID_ERROR_CODE";
    }
  }
}
