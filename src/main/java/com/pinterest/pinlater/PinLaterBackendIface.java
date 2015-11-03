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

import com.pinterest.pinlater.thrift.PinLaterCheckpointJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterDeleteJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterDequeueRequest;
import com.pinterest.pinlater.thrift.PinLaterDequeueResponse;
import com.pinterest.pinlater.thrift.PinLaterEnqueueRequest;
import com.pinterest.pinlater.thrift.PinLaterEnqueueResponse;
import com.pinterest.pinlater.thrift.PinLaterGetJobCountRequest;
import com.pinterest.pinlater.thrift.PinLaterJobAckRequest;
import com.pinterest.pinlater.thrift.PinLaterJobInfo;
import com.pinterest.pinlater.thrift.PinLaterLookupJobRequest;
import com.pinterest.pinlater.thrift.PinLaterRetryFailedJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterScanJobsRequest;
import com.pinterest.pinlater.thrift.PinLaterScanJobsResponse;

import com.twitter.util.Future;

import java.util.Map;
import java.util.Set;

/**
 * PinLater backend API. Currently, directly mirrors the PinLater thrift service API.
 * Backend implementations should be async and non-blocking.
 */
public interface PinLaterBackendIface {

  Future<Void> createQueue(String name);

  Future<Void> deleteQueue(String name, String password);

  Future<PinLaterEnqueueResponse> enqueueJobs(PinLaterEnqueueRequest request);

  Future<PinLaterDequeueResponse> dequeueJobs(String source, PinLaterDequeueRequest request);

  Future<Void> ackDequeuedJobs(PinLaterJobAckRequest request);

  Future<Void> checkpointJobs(String source, PinLaterCheckpointJobsRequest request);

  Future<Map<String, PinLaterJobInfo>> lookupJobs(PinLaterLookupJobRequest request);

  Future<Integer> getJobCount(PinLaterGetJobCountRequest request);

  Future<Set<String>> getQueueNames();

  Future<PinLaterScanJobsResponse> scanJobs(PinLaterScanJobsRequest request);

  Future<Integer> retryFailedJobs(PinLaterRetryFailedJobsRequest request);

  Future<Integer> deleteJobs(PinLaterDeleteJobsRequest request);
}