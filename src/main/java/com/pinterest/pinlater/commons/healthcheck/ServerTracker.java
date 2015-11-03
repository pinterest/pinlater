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
package com.pinterest.pinlater.commons.healthcheck;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.ostrich.stats.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that implements the server health status tracking.
 *
 * It keeps the way of how to do heartbeat to the server and also the current status of the server.
 * The algorithm of detecting server live/dead is based upon configurable consecutive
 * failures/successes.
 *
 * Only reportHeartbeatResult() API is synchronized so as to protect the consistency of internal
 * state. isLive() is not synchronized, and the visibility of ``isLive`` is guaranteed by using
 * volatile.
 */
public class ServerTracker {

  private static final Logger LOG = LoggerFactory.getLogger(ServerTracker.class);

  private final String host;
  private final int port;
  private final HeartBeater heartBeater;
  private final int consecutiveFailures;
  private final int consecutiveSuccesses;
  public final int pingIntervalSecs;
  private volatile boolean isLive = true;

  private int currentConsecutiveFailures;
  private int currentConsecutiveSuccesses;

  // Track how many simulated seconds have lapsed in test after this tracker is initialized.
  @VisibleForTesting
  public int totalSecondsLapsed = 0;

  public ServerTracker(String host,
                       int port,
                       HeartBeater heartBeater,
                       int consecutiveFailures,
                       int consecutiveSuccesses,
                       int pingIntervalSecs,
                       boolean isLiveInitially) {
    this.host = host;
    this.port = port;
    this.heartBeater = heartBeater;
    this.consecutiveFailures = consecutiveFailures;
    this.consecutiveSuccesses = consecutiveSuccesses;
    this.pingIntervalSecs = pingIntervalSecs;
    this.isLive = isLiveInitially;
  }

  /**
   * Ping the server.
   *
   * Returns:
   *     True if the server is live. Otherwise either return False or throw an exception to
   *     indicate a failed heartbeat
   */
  public boolean heartbeat() throws Exception {
    return heartBeater.ping();
  }

  /**
   * Report the result of a heartbeat.
   *
   * Args:
   *   isLive: whether the heartbeat is successful.
   */
  public void reportHeartbeatResult(boolean isLive) {
    if (isLive) {
      reportHeartbeatSuccess();
    } else {
      reportHeartbeatFailure();
    }
  }

  /**
   * Report a successful heartbeat.
   */
  private synchronized void reportHeartbeatSuccess() {
    currentConsecutiveFailures = 0;
    currentConsecutiveSuccesses += 1;

    if (isLive) {
      return;
    }

    if (currentConsecutiveSuccesses >= consecutiveSuccesses) {
      LOG.info(String.format("Server %s:%d is determined as live by health check.", host, port));
      isLive = true;
    }
  }

  /**
   * Report a failed heartbeat.
   */
  private synchronized void reportHeartbeatFailure() {
    Stats.incr(String.format("heartbeat_failures_%s_%d", host, port));
    currentConsecutiveSuccesses = 0;
    currentConsecutiveFailures += 1;

    if (!isLive) {
      // Do not LOG here to prevent noise.
      Stats.incr(String.format("healthcheck_dead_%s_%d", host, port));
      return;
    }

    if (currentConsecutiveFailures >= consecutiveFailures) {
      LOG.info(String.format("Server %s:%d is determined as dead by health check.", host, port));
      Stats.incr(String.format("healthcheck_dead_%s_%d", host, port));
      isLive = false;
    }
  }

  /**
   * Returns whether the server is live now.
   */
  public boolean isLive() {
    return isLive;
  }
}
