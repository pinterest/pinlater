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

import com.google.common.base.Preconditions;
import com.twitter.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple utility to keep track of and periodically log QPS and latency for a client or server.
 */
public class RPCStatsLogger {

  private static final Logger LOG = LoggerFactory.getLogger(RPCStatsLogger.class);

  private final int logIntervalSeconds;
  private long queriesIssued = 0;
  private long totalLatencyMicros = 0;
  private long lastLogTimestampMillis = 0;

  public RPCStatsLogger(int logIntervalSeconds) {
    this.logIntervalSeconds = Preconditions.checkNotNull(logIntervalSeconds);
  }

  /**
   * Call this when each request completes.
   *
   * @param latency Latency for that request.
   */
  public synchronized void requestComplete(Duration latency) {
    queriesIssued++;
    totalLatencyMicros += latency.inMicroseconds();

    long currentTimeMillis = System.currentTimeMillis();
    if (currentTimeMillis >= lastLogTimestampMillis + logIntervalSeconds * 1000) {
      long qps = queriesIssued * 1000 / (currentTimeMillis - lastLogTimestampMillis);
      double avgLatencyMillis = totalLatencyMicros / (queriesIssued * 1000.0);
      LOG.info(String.format("QPS: %d Avg Latency (ms): %.5f", qps, avgLatencyMillis));

      queriesIssued = 0;
      totalLatencyMicros = 0;
      lastLogTimestampMillis = currentTimeMillis;
    }
  }
}
