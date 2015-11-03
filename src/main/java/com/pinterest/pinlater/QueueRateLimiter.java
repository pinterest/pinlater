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

import com.google.common.util.concurrent.RateLimiter;

/**
 * Simple wrapper class for Guava's RateLimiter. This is necessary because the Guava
 * implementation does not allow rate to be zero, something which we need.
 */
public final class QueueRateLimiter {

  private QueueRateLimiter() {}

  public static interface IFace {

    public boolean allowDequeue(int numJobs);

    public double getRate();
  }

  public static IFace create(double maxRequestsPerSecond) {
    if (maxRequestsPerSecond <= 0.0) {
      return ALLOW_NONE;
    }

    final RateLimiter rateLimiter = RateLimiter.create(maxRequestsPerSecond);
    return new IFace() {
      @Override
      public boolean allowDequeue(int numJobs) {
        return rateLimiter.tryAcquire(numJobs);
      }

      @Override
      public double getRate() {
        return rateLimiter.getRate();
      }
    };
  }

  private static final IFace ALLOW_NONE = new IFace() {
    @Override
    public boolean allowDequeue(int numJobs) {
      return false;
    }

    @Override
    public double getRate() {
      return 0.0;
    }
  };
}
