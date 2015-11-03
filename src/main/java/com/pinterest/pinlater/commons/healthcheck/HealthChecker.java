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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A generic health check class that provide background monitoring for servers' health.
 *
 * For each server, the health checker 'pings' the server in the specified interval, and provides
 * API to get the current status of server. The algorithm of detecting server live/dead is done in
 * ``ServerTracker`` class, which is simply based upon configurable consecutive failures/successes.
 *
 * The caller can also choose to not have health checker do background monitoring by passing 0
 * pingIntervalSecs to addServer(). Then the caller can report heart beat result itself by calling
 * reportHeartbeat().
 */
public class HealthChecker {

  private static final Logger LOG = LoggerFactory.getLogger(HealthChecker.class);
  private static final Random RANDOM = new Random();

  // Read/write lock to protect thread safety of accessing ``serverTrackerMap`` and
  // ``scheduledFutureMap``.
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  // Server name to server tracker.
  private final Map<String, ServerTracker> serverTrackerMap = Maps.newHashMap();
  // Server name to scheduled future.
  private final Map<String, ScheduledFuture<?>> scheduledFutureMap = Maps.newHashMap();

  private final ScheduledExecutorService scheduledExecutorService;

  /**
   * @param name name of the health checker, which is appended to the thread name.
   */
  public HealthChecker(String name) {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(name + "-healthchecker-%d")
            .build());
  }


  private String getServerName(String host, int port) {
    return String.format("%s:%d", host, port);
  }

  /**
   * Add server to be under health check in background.
   *
   * @param host hostname of the server.
   * @param port port of the server.
   * @param heartBeater method to ping the server to do heart beat.
   * @param consecutiveFailures number of consecutive failures before we regard server as dead.
   * @param consecutiveSuccesses number of consecutive successes before we regard server as live.
   * @param pingIntervalSecs interval to ping the server. If the caller wants to do heartbeat and
   *                         report heartbeat result by himself. Pass 0 here.
   * @param isHealthyInitially healthy or not initially.
   * @return True if successfully added. False if this server is also under health check.
   */
  public boolean addServer(String host,
                           int port,
                           HeartBeater heartBeater,
                           int consecutiveFailures,
                           int consecutiveSuccesses,
                           int pingIntervalSecs,
                           boolean isHealthyInitially) {
    String serverName = getServerName(host, port);
    lock.writeLock().lock();
    try {
      if (serverTrackerMap.containsKey(serverName)) {
        LOG.warn(String.format("Server %s already exists in health check list. Ignore.",
            serverName));
        return false;
      }
      ServerTracker serverTracker = new ServerTracker(
          host,
          port,
          heartBeater,
          consecutiveFailures,
          consecutiveSuccesses,
          pingIntervalSecs,
          isHealthyInitially);
      serverTrackerMap.put(serverName, serverTracker);
      // Schedule fixed delay runnable if ``pingIntervalSecs`` > 0.
      if (pingIntervalSecs > 0) {
        ScheduledFuture<?> scheduledFuture = scheduledExecutorService.scheduleWithFixedDelay(
            new HealthCheckRunnable(serverName),
            // Randomize initial delay to prevent all servers from running health check at the same
            // time.
            pingIntervalSecs + RANDOM.nextInt(pingIntervalSecs),
            pingIntervalSecs,
            TimeUnit.SECONDS);
        scheduledFutureMap.put(serverName, scheduledFuture);
      }
    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }

  /**
   * Remove server from health check.
   *
   * @param host hostname of the server.
   * @param port port of the server.
   * @return True if successfully removed. False if this server is not under health check.
   */
  public boolean removeServer(String host, int port) {
    String serverName = getServerName(host, port);
    lock.writeLock().lock();
    try {
      if (!serverTrackerMap.containsKey(serverName)) {
        LOG.warn(String.format("Server %s does not exist in health check list. Ignore.",
            serverName));
        return false;
      }

      if (scheduledFutureMap.containsKey(serverName)) {
        scheduledFutureMap.get(serverName).cancel(true);
        scheduledFutureMap.remove(serverName);
      }
      serverTrackerMap.remove(serverName);
    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }

  /**
   * Get the live status of the server.
   *
   * @param host hostname of the server.
   * @param port port of the server.
   * @return True if server is live, false otherwise.
   */
  public boolean isServerLive(String host, int port) {
    lock.readLock().lock();
    try {
      String serverName = getServerName(host, port);
      Preconditions.checkArgument(serverTrackerMap.containsKey(serverName));
      return serverTrackerMap.get(getServerName(host, port)).isLive();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Check the health status of the server.
   *
   * @param serverName string representation of server host and port, which can be obtained from
   *                   getServerName().
   */
  private void checkServer(String serverName) {
    lock.readLock().lock();
    try {
      boolean isHealthy = false;
      ServerTracker serverTracker = serverTrackerMap.get(serverName);
      try {
        isHealthy = serverTracker.heartbeat();
      } catch (Exception e) {
      }
      serverTracker.reportHeartbeatResult(isHealthy);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns whether the server is under health check.
   *
   * @param host hostname of the server.
   * @param port port of the server.
   * @return True if server is under health check.
   */
  public boolean isServerMonitored(String host, int port) {
    lock.readLock().lock();
    try {
      String serverName = getServerName(host, port);
      return serverTrackerMap.containsKey(serverName);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Report a heartbeat status of a server.
   *
   * This function can be used to report heartbeats by external callers in addition to the regular
   * pings maintained by this class.
   *
   * @param host hostname of the server.
   * @param port port of the server.
   * @param success heartbeat result.
   */
  public void reportHeartbeat(String host, int port, boolean success) {
    lock.readLock().lock();
    try {
      String serverName = getServerName(host, port);
      Preconditions.checkArgument(serverTrackerMap.containsKey(serverName));
      serverTrackerMap.get(serverName).reportHeartbeatResult(success);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Shut down the health checker.
   */
  public void shutdown() {
    scheduledExecutorService.shutdownNow();
  }

  /**
   * Runnable implementation which is called by the health checker scheduler thread every interval
   * seconds. It will do the one heartbeat for its server.
   */
  private class HealthCheckRunnable implements Runnable {

    private final String serverName;

    public HealthCheckRunnable(String serverName) {
      this.serverName = serverName;
    }

    @Override
    public void run() {
      checkServer(serverName);
    }
  }

  /**
   * This method simulates seconds have lapsed by doing specific number of health checks for each
   * server respectively.
   *
   * Note: This method is not thread safe.
   */
  @VisibleForTesting
  public void simulatSecondsLapsed(int seconds) {
    for (String serverName : serverTrackerMap.keySet()) {
      int pingIntervalSecs = serverTrackerMap.get(serverName).pingIntervalSecs;
      if (pingIntervalSecs <= 0) {
        continue;
      }
      int totalSecondsLapsed = serverTrackerMap.get(serverName).totalSecondsLapsed;
      int newTotalSecondsLapsed = totalSecondsLapsed + seconds;
      for (int i = 0;
           i < (newTotalSecondsLapsed / pingIntervalSecs)
               - (totalSecondsLapsed / pingIntervalSecs);
           i++) {
        checkServer(serverName);
      }
      serverTrackerMap.get(serverName).totalSecondsLapsed = newTotalSecondsLapsed;
    }
  }
}
