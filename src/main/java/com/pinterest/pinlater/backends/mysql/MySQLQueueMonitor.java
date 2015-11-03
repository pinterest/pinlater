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
package com.pinterest.pinlater.backends.mysql;

import com.pinterest.pinlater.backends.common.BackendQueueMonitorBase;
import com.pinterest.pinlater.commons.jdbc.JdbcUtils;
import com.pinterest.pinlater.thrift.PinLaterJobState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.twitter.ostrich.stats.Stats;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;

/**
 * Implements a scheduled task used by the PinLaterMySQLBackend for various types of
 * job queue cleanup, including ACK timeouts and GC'ing finished jobs.
 */
public class MySQLQueueMonitor extends BackendQueueMonitorBase<MySQLDataSources> {

  private static final Logger LOG = LoggerFactory.getLogger(PinLaterMySQLBackend.class);
  private int logCount;
  private int numTimeoutDone;
  private int numTimeoutRetry;
  private int numSucceededGC;
  private int numFailedGC;

  public MySQLQueueMonitor(ImmutableMap<String, MySQLDataSources> shardMap,
                           PropertiesConfiguration configuration) {
    super(shardMap, configuration);
  }

  @VisibleForTesting
  public MySQLQueueMonitor(ImmutableMap<String, MySQLDataSources> shardMap,
                           int updateMaxSize, int maxAutoRetries, int logInterval,
                           long jobClaimedTimeoutMillis, long jobSucceededGCTimeoutMillis,
                           long jobFailedGCTimeoutMillis,
                           int numPriorityLevels) {
    super(shardMap, updateMaxSize, maxAutoRetries, logInterval, jobClaimedTimeoutMillis,
        jobSucceededGCTimeoutMillis, jobFailedGCTimeoutMillis, numPriorityLevels);
  }

  @Override
  public void run() {
    final long runStartMillis = System.currentTimeMillis();
    for (ImmutableMap.Entry<String, MySQLDataSources> shard : getShardMap().entrySet()) {
      jobMonitorImpl(runStartMillis, shard, getMaxAutoRetries());
    }
  }

  @Override
  protected void jobMonitorImpl(long runStartMillis,
                                Map.Entry<String, MySQLDataSources> shard,
                                int numAutoRetries) {
    Connection conn = null;
    ResultSet dbNameRs = null;
    Timestamp succeededGCTimestamp = new Timestamp(
        runStartMillis - getJobSucceededGCTimeoutMillis());
    Timestamp failedGCTimestamp = new Timestamp(runStartMillis - getJobFailedGCTimeoutMillis());
    Timestamp timeoutTimestamp = new Timestamp(runStartMillis - getJobClaimedTimeoutMillis());

    try {
      conn = shard.getValue().getMonitorDataSource().getConnection();
      Set<String> queueNames = MySQLBackendUtils.getQueueNames(conn, shard.getKey());
      for (String queueName : queueNames) {
        for (int priority = 1;
             priority <= numPriorityLevels;
             priority++) {
          String jobsTableName = MySQLBackendUtils.constructJobsTableName(
              queueName, shard.getKey(), priority);

          // Handle timed out jobs with attempts exhausted.
          numTimeoutDone += JdbcUtils.executeUpdate(
              conn,
              String.format(MySQLQueries.MONITOR_TIMEOUT_DONE_UPDATE, jobsTableName),
              PinLaterJobState.FAILED.getValue(),
              PinLaterJobState.IN_PROGRESS.getValue(),
              timeoutTimestamp,
              getUpdateMaxSize());

          // Handle timed out jobs with attempts remaining.
          numTimeoutRetry += JdbcUtils.executeUpdate(
              conn,
              String.format(MySQLQueries.MONITOR_TIMEOUT_RETRY_UPDATE, jobsTableName),
              PinLaterJobState.PENDING.getValue(),
              PinLaterJobState.IN_PROGRESS.getValue(),
              timeoutTimestamp,
              getUpdateMaxSize());

          // Succeeded job GC.
          numSucceededGC += JdbcUtils.executeUpdate(
              conn,
              String.format(MySQLQueries.MONITOR_GC_DONE_JOBS, jobsTableName),
              PinLaterJobState.SUCCEEDED.getValue(),
              succeededGCTimestamp,
              getUpdateMaxSize());

          // Failed job GC.
          numFailedGC += JdbcUtils.executeUpdate(
              conn,
              String.format(MySQLQueries.MONITOR_GC_DONE_JOBS, jobsTableName),
              PinLaterJobState.FAILED.getValue(),
              failedGCTimestamp,
              getUpdateMaxSize());

          logCount++;
          if (logCount % getLogInterval() == 0) {
            LOG.info(String.format(
                "JobQueueMonitor: "
                    + "Shard: %s Queue: %s Priority: %d Timeout Done: %d Timeout Retry: %d "
                    + "Succeeded GC: %d Failed GC: %d",
                shard.getKey(), queueName, priority, numTimeoutDone, numTimeoutRetry,
                numSucceededGC, numFailedGC));
            Stats.incr(queueName + "_timeout_done", numTimeoutDone);
            Stats.incr(queueName + "_timeout_retry", numTimeoutRetry);
            Stats.incr(queueName + "_succeeded_gc", numSucceededGC);
            Stats.incr(queueName + "_failed_gc", numFailedGC);
            logCount = 0;
            numTimeoutDone = 0;
            numTimeoutRetry = 0;
            numSucceededGC = 0;
            numFailedGC = 0;
          }
        }
      }
    } catch (Exception e) {
      // Deadlocks are occasionally expected for our high-contention queries. We
      // retry a few times so as not to abort an entire monitor cycle.
      if (MySQLBackendUtils.isDeadlockException(e) && numAutoRetries > 0) {
        Stats.incr("mysql-deadlock-monitor");
        jobMonitorImpl(runStartMillis, shard, numAutoRetries - 1);
      } else {
        LOG.error("Exception in JobQueueMonitor task", e);
      }
    } finally {
      JdbcUtils.closeResultSet(dbNameRs);
      JdbcUtils.closeConnection(conn);
    }
  }
}
