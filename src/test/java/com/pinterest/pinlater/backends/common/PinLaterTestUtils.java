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
import com.pinterest.pinlater.thrift.PinLaterGetJobCountRequest;
import com.pinterest.pinlater.thrift.PinLaterJobState;

public class PinLaterTestUtils {

  /**
   * Convenient interface to getJobCount to make testing a little less verbose.
   *
   * @param queueName Name of queue to count jobs in.
   * @param jobState State of jobs to look for.
   * @return An int as job count.
   */
  public static int getJobCount(PinLaterBackendBase backend, final String queueName,
                                final PinLaterJobState jobState) {
    return backend.getJobCount(new PinLaterGetJobCountRequest(queueName,
        jobState)).get().intValue();
  }
}
