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

import com.pinterest.pinlater.PinLaterBackendBase;
import com.pinterest.pinlater.backends.common.PinLaterBackendUtils;
import com.pinterest.pinlater.backends.common.PinLaterJobDescriptor;
import com.pinterest.pinlater.commons.jdbc.JdbcUtils;
import com.pinterest.pinlater.commons.jdbc.RowProcessor;
import com.pinterest.pinlater.commons.jdbc.SingleColumnRowProcessor;
import com.pinterest.pinlater.thrift.ErrorCode;
import com.pinterest.pinlater.thrift.PinLaterCheckpointJobRequest;
import com.pinterest.pinlater.thrift.PinLaterDequeueMetadata;
import com.pinterest.pinlater.thrift.PinLaterDequeueResponse;
import com.pinterest.pinlater.thrift.PinLaterException;
import com.pinterest.pinlater.thrift.PinLaterJob;
import com.pinterest.pinlater.thrift.PinLaterJobAckInfo;
import com.pinterest.pinlater.thrift.PinLaterJobInfo;
import com.pinterest.pinlater.thrift.PinLaterJobState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.ostrich.stats.Stats;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple6;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * PinLater backend implementation that uses MySQL as the underlying store.
 *
 * Backend specific behavior:
 *  1. MySQL backend ensure that the custom status stored in db should not exceed
 *     ``CUSTOM_STATUS_SIZE_BYTES``.
 *  2. MySQL backend limits the count of jobs returned to ``MYSQL_COUNT_LIMIT``.
 */
public class PinLaterMySQLBackend extends PinLaterBackendBase {

  private static final Logger LOG = LoggerFactory.getLogger(PinLaterMySQLBackend.class);

  private final AtomicReference<ImmutableMap<String, MySQLDataSources>> shardMapRef =
      new AtomicReference<ImmutableMap<String, MySQLDataSources>>();
  private final int countLimit;
  private final int numDbPerQueue;
  private final MySQLHealthMonitor mySQLHealthMonitor;
  private final PropertiesConfiguration configuration;
  private final ScheduledExecutorService queueMonitorService;
  private volatile ScheduledFuture<?> queueMonitorFuture;

  /**
   * Creates an instance of the PinLaterMySQLBackend.
   *
   * @param configuration          configuration parameters for the backend.
   * @param serverHostName         hostname of the PinLater server.
   * @param serverStartTimeMillis  start time of the PinLater server.
   */
  public PinLaterMySQLBackend(PropertiesConfiguration configuration,
                              String serverHostName,
                              long serverStartTimeMillis) throws Exception {
    super(configuration, "MySQL", serverHostName, serverStartTimeMillis);
    this.configuration = Preconditions.checkNotNull(configuration);
    this.countLimit = configuration.getInt("MYSQL_COUNT_LIMIT");
    this.numDbPerQueue = configuration.getInt("MYSQL_NUM_DB_PER_QUEUE", 1);
    this.mySQLHealthMonitor = new MySQLHealthMonitor(new HashSet<String>());

    // Start the JobQueueMonitor scheduled task.
    this.queueMonitorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("MySQLJobQueueMonitor-%d")
            .build());

    // Call Base class's initialization function to initialize the shardMap, futurePool and dequeue
    // semaphoreMap.
    initialize();
  }

  @Override
  protected ImmutableSet<String> getShards() {
    return shardMapRef.get().keySet();
  }

  @Override
  @VisibleForTesting
  protected void processConfigUpdate(byte[] bytes) throws Exception {
    MySQLConfigSchema mysqlConfig;
    mysqlConfig = MySQLConfigSchema.read(
        new ByteArrayInputStream(bytes),
        configuration.getString("SHARD_ALLOWED_HOSTNAME_PREFIX"));

    ImmutableMap.Builder<String, MySQLDataSources> shardMapBuilder =
        new ImmutableMap.Builder<String, MySQLDataSources>();
    ImmutableMap<String, MySQLDataSources> currentMap = shardMapRef.get();
    Map<String, Boolean> updatedMap = Maps.newHashMap();

    Set<String> queueNames = getQueueNamesImpl();
    // Check if we can reuse any old MySQLDataSource object. updatedMap indicates if a shard's
    // config has changed or it's a new shard (in these cases we'll need to initialize new
    // connections to the shard).
    for (MySQLConfigSchema.Shard shard : mysqlConfig.shards) {
      // Note: we only need to check if we can reuse the connection to the first database for each
      // MySQL instance. The number of databases per queue can't change at runtime, and all the
      // databases in the same MySQL instance share a same data source. So if the
      // connection to database 0 is reusable, it is guaranteed that we can reuse if for all other
      // databases within the same instance.
      String shardName = MySQLBackendUtils.constructShardName(shard.id, 0);
      if (currentMap != null && currentMap.containsKey(shardName)
          && currentMap.get(shardName).needNewConnection(
          shard.shardConfig.master.host,
          shard.shardConfig.master.port,
          shard.shardConfig.user,
          shard.shardConfig.passwd)) {
        MySQLDataSources ds = currentMap.get(shardName);
        ds.setDequeueOnly(shard.shardConfig.dequeueOnly);
        for (int dbId = 0; dbId < numDbPerQueue; dbId++) {
          shardName = MySQLBackendUtils.constructShardName(shard.id, dbId);
          shardMapBuilder.put(shardName, ds);
          updatedMap.put(shardName, false);
        }
      } else {
        MySQLDataSources ds = new MySQLDataSources(
            configuration,
            shard.shardConfig.master.host,
            shard.shardConfig.master.port,
            shard.shardConfig.user,
            shard.shardConfig.passwd,
            shard.shardConfig.dequeueOnly);
        for (int dbId = 0; dbId < numDbPerQueue; dbId++) {
          shardName = MySQLBackendUtils.constructShardName(shard.id, dbId);
          shardMapBuilder.put(shardName, ds);
          updatedMap.put(shardName, true);
        }
      }
    }
    mySQLHealthMonitor.updateHealthMap(updatedMap);
    shardMapRef.set(shardMapBuilder.build());
    // Need to create queues for new added shards
    for (String queueName : queueNames) {
      createQueueImpl(queueName);
    }

    // Restart the JobQueueMonitor scheduled task.
    int delaySeconds = configuration.getInt("BACKEND_MONITOR_THREAD_DELAY_SECONDS");
    if (this.queueMonitorFuture != null) {
      this.queueMonitorFuture.cancel(true);
    }
    this.queueMonitorFuture = this.queueMonitorService.scheduleWithFixedDelay(
        new MySQLQueueMonitor(shardMapRef.get(), configuration),
        // Randomize initial delay to prevent all servers from running GC at the same time.
        delaySeconds + RANDOM.nextInt(delaySeconds),
        delaySeconds,
        TimeUnit.SECONDS);
    LOG.info("MySQL config update, new value: {}", mysqlConfig);
  }

  @VisibleForTesting
  ImmutableMap<String, MySQLDataSources> getEnqueueableShards() {
    ImmutableMap.Builder<String, MySQLDataSources> shardMapBuilder =
        new ImmutableMap.Builder<String, MySQLDataSources>();

    for (ImmutableMap.Entry<String, MySQLDataSources> shard : shardMapRef.get().entrySet()) {
      if (!shard.getValue().isDequeueOnly()) {
        shardMapBuilder.put(shard);
      }
    }

    return shardMapBuilder.build();
  }

  @Override
  protected void createQueueImpl(final String queueName) throws Exception {
    for (ImmutableMap.Entry<String, MySQLDataSources> shard : shardMapRef.get().entrySet()) {
      Connection conn = null;
      String dbName = MySQLBackendUtils.constructDBName(queueName, shard.getKey());
      try {
        // We share the data source with enqueue.
        conn = shard.getValue().getGeneralDataSource().getConnection();

        // Create the database.
        JdbcUtils.executeUpdate(
            conn,
            String.format(MySQLQueries.CREATE_DATABASE, dbName));

        // Create the queue tables, one for each priority level.
        for (int priority = 1;
             priority <= numPriorityLevels;
             priority++) {
          JdbcUtils.executeUpdate(
              conn,
              String.format(
                  MySQLQueries.CREATE_JOBS_TABLE,
                  MySQLBackendUtils.constructJobsTableName(queueName, shard.getKey(), priority)));
        }
      } catch (SQLException e) {
        // If database already exists, then just ignore this and move onto the next shard.
        if (MySQLBackendUtils.isDatabaseAlreadyExistsException(e)) {
          continue;
        }

        // Wrap any other recognized exceptions as a PinLaterException.
        if (MySQLBackendUtils.isDatabaseNameTooLongException(e)) {
          throw new PinLaterException(ErrorCode.QUEUE_NAME_TOO_LONG,
              String.format(
                  "Queue name is too long by %d characters. Attempted to create queue DB '%s'"
                      + " but it's longer than maximum allowed %d characters.",
                  dbName.length() - MySQLBackendUtils.MYSQL_MAX_DB_NAME_LENGTH,
                  dbName,
                  MySQLBackendUtils.MYSQL_MAX_DB_NAME_LENGTH));
        }

        throw e;
      } finally {
        JdbcUtils.closeConnection(conn);
      }
    }
  }

  @Override
  protected void deleteQueueImpl(final String queueName) throws Exception {
    for (ImmutableMap.Entry<String, MySQLDataSources> shard : shardMapRef.get().entrySet()) {
      Connection conn = null;
      try {
        conn = shard.getValue().getGeneralDataSource().getConnection();
        JdbcUtils.executeUpdate(
            conn,
            String.format(MySQLQueries.DROP_DATABASE,
                MySQLBackendUtils.constructDBName(queueName, shard.getKey())));
      } catch (SQLException e) {
        // If database does not exist, then just ignore this and move onto the next shard.
        if (MySQLBackendUtils.isDatabaseDoesNotExistException(e)) {
          continue;
        }
        throw e;
      } finally {
        JdbcUtils.closeConnection(conn);
      }
    }
  }

  @Override
  protected PinLaterJobInfo lookupJobFromShard(
      final String queueName,
      final String shardName,
      final int priority,
      final long localId,
      final boolean isIncludeBody) throws Exception {
    final String mySQLQuery = isIncludeBody ? MySQLQueries.LOOKUP_JOB_WITH_BODY :
                              MySQLQueries.LOOKUP_JOB;
    String jobsTableName = MySQLBackendUtils.constructJobsTableName(queueName, shardName, priority);
    Connection conn = null;
    ImmutableMap<String, MySQLDataSources> shardMap = shardMapRef.get();
    try {
      conn = shardMap.get(shardName).getGeneralDataSource().getConnection();
      PinLaterJobInfo jobInfo = JdbcUtils.selectOne(
          conn,
          String.format(mySQLQuery, jobsTableName),
          new RowProcessor<PinLaterJobInfo>() {
            @Override
            public PinLaterJobInfo process(ResultSet rs) throws IOException, SQLException {
              PinLaterJobInfo ji = new PinLaterJobInfo();
              ji.setJobDescriptor(
                  new PinLaterJobDescriptor(
                      queueName,
                      shardName,
                      priority,
                      rs.getLong(1)).toString());
              ji.setJobState(PinLaterJobState.findByValue(rs.getInt(2)));
              ji.setAttemptsAllowed(rs.getInt(3));
              ji.setAttemptsRemaining(rs.getInt(4));
              ji.setCreatedAtTimestampMillis(rs.getTimestamp(5).getTime());
              ji.setRunAfterTimestampMillis(rs.getTimestamp(6).getTime());
              ji.setUpdatedAtTimestampMillis(rs.getTimestamp(7).getTime());
              String claimDescriptor = rs.getString(8);
              if (claimDescriptor != null) {
                ji.setClaimDescriptor(claimDescriptor);
              }
              ;
              ji.setCustomStatus(Strings.nullToEmpty(rs.getString(9)));
              if (isIncludeBody) {
                ji.setBody(rs.getBytes(10));
              }
              return ji;
            }
          },
          localId);
      return jobInfo;
    } finally {
      JdbcUtils.closeConnection(conn);
    }
  }

  @Override
  protected int getJobCountFromShard(
      final String queueName,
      final String shardName,
      final Set<Integer> priorities,
      final PinLaterJobState jobState,
      final boolean countFutureJobs,
      final String bodyRegexTomatch) throws Exception {
    final String countQuery = countFutureJobs ? MySQLQueries.COUNT_FUTURE_JOBS_BY_STATE_PRIORITY :
                              MySQLQueries.COUNT_CURRENT_JOBS_BY_STATE_PRIORITY;
    int totalCount = 0;
    Connection conn = null;
    try {
      ImmutableMap<String, MySQLDataSources> shardMap = shardMapRef.get();
      conn = shardMap.get(shardName).getGeneralDataSource().getConnection();
      for (int priority : priorities) {
        Integer count = JdbcUtils.selectOne(
            conn,
            String.format(countQuery,
                MySQLBackendUtils.constructJobsTableName(queueName, shardName, priority),
                getBodyRegexClause(bodyRegexTomatch)),
            new SingleColumnRowProcessor<Integer>(Integer.class),
            jobState.getValue(),
            countLimit);
        if (count != null) {
          totalCount += count;
        }
      }
    } finally {
      JdbcUtils.closeConnection(conn);
    }
    return totalCount;
  }

  @Override
  protected Set<String> getQueueNamesImpl() throws SQLException {
    ImmutableMap.Entry<String, MySQLDataSources> randomShard = getRandomShard();
    if (randomShard == null) {
      return Sets.newHashSet();
    }

    Connection conn = null;
    try {
      conn = randomShard.getValue().getGeneralDataSource().getConnection();
      return MySQLBackendUtils.getQueueNames(conn, randomShard.getKey());
    } finally {
      JdbcUtils.closeConnection(conn);
    }
  }

  @Override
  protected List<PinLaterJobInfo> scanJobsFromShard(
      final String queueName,
      final String shardName,
      final Set<Integer> priorities,
      final PinLaterJobState jobState,
      final boolean scanFutureJobs,
      final String continuation,
      final int limit,
      final String bodyRegexTomatch) throws Exception {
    final String scanQuery = scanFutureJobs ? MySQLQueries.SCAN_FUTURE_JOBS :
                             MySQLQueries.SCAN_CURRENT_JOBS;
    Connection conn = null;
    try {
      ImmutableMap<String, MySQLDataSources> shardMap = shardMapRef.get();
      conn = shardMap.get(shardName).getGeneralDataSource().getConnection();

      // First scan some jobs for the specified priorities.
      List<List<PinLaterJobInfo>> jobsPerPriority =
          Lists.newArrayListWithCapacity(priorities.size());
      for (final int priority : priorities) {
        jobsPerPriority.add(
            JdbcUtils.select(
                conn,
                String.format(scanQuery,
                    MySQLBackendUtils.constructJobsTableName(queueName, shardName, priority),
                    getBodyRegexClause(bodyRegexTomatch)),
                new RowProcessor<PinLaterJobInfo>() {
                  @Override
                  public PinLaterJobInfo process(ResultSet rs) throws IOException, SQLException {
                    PinLaterJobInfo ji = new PinLaterJobInfo();
                    ji.setJobDescriptor(
                        new PinLaterJobDescriptor(
                            queueName,
                            shardName,
                            priority,
                            rs.getLong(1)).toString());
                    String claimDescriptor = rs.getString(2);
                    if (claimDescriptor != null) {
                      ji.setClaimDescriptor(claimDescriptor);
                    }
                    ji.setAttemptsAllowed(rs.getInt(3));
                    ji.setAttemptsRemaining(rs.getInt(4));
                    ji.setCustomStatus(Strings.nullToEmpty(rs.getString(5)));
                    ji.setCreatedAtTimestampMillis(rs.getTimestamp(6).getTime());
                    ji.setRunAfterTimestampMillis(rs.getTimestamp(7).getTime());
                    ji.setUpdatedAtTimestampMillis(rs.getTimestamp(8).getTime());
                    ji.setJobState(jobState);
                    return ji;
                  }
                },
                jobState.getValue(),
                limit));
      }

      // Merge jobsPerPriority and return the merged result.
      return PinLaterBackendUtils.mergeIntoList(
          jobsPerPriority, PinLaterBackendUtils.JobInfoComparator.getInstance());
    } finally {
      JdbcUtils.closeConnection(conn);
    }
  }

  @Override
  protected int retryFailedJobsFromShard(
      final String queueName,
      final String shardName,
      final int priority,
      final int attemptsRemaining,
      final long runAfterTimestampMillis,
      final int limit) throws Exception {
    Connection conn = null;
    try {
      ImmutableMap<String, MySQLDataSources> shardMap = shardMapRef.get();
      conn = shardMap.get(shardName).getGeneralDataSource().getConnection();
      return JdbcUtils.executeUpdate(
          conn,
          String.format(MySQLQueries.RETRY_FAILED_JOBS,
              MySQLBackendUtils.constructJobsTableName(queueName, shardName, priority)),
          PinLaterJobState.PENDING.getValue(),
          attemptsRemaining,
          new Timestamp(runAfterTimestampMillis),
          PinLaterJobState.FAILED.getValue(),
          limit);
    } finally {
      JdbcUtils.closeConnection(conn);
    }
  }

  @Override
  protected int deleteJobsFromShard(
      final String queueName,
      final String shardName,
      final PinLaterJobState jobState,
      final int priority,
      final String bodyRegexTomatch,
      final int limit) throws Exception {
    Connection conn = null;
    try {
      ImmutableMap<String, MySQLDataSources> shardMap = shardMapRef.get();
      conn = shardMap.get(shardName).getGeneralDataSource().getConnection();
      return JdbcUtils.executeUpdate(
          conn,
          String.format(MySQLQueries.DELETE_JOBS,
              MySQLBackendUtils.constructJobsTableName(queueName, shardName, priority),
              getBodyRegexClause(bodyRegexTomatch)),
          jobState.getValue(),
          limit);
    } finally {
      JdbcUtils.closeConnection(conn);
    }
  }

  @Override
  protected String enqueueSingleJob(String queueName, PinLaterJob job, int numAutoRetries)
      throws Exception {
    final long currentTimeMillis = System.currentTimeMillis();
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    final ImmutableMap.Entry<String, MySQLDataSources> shard = getRandomEnqueueableShard();
    try {
      conn = shard.getValue().getGeneralDataSource().getConnection();
      String jobsTableName =
          MySQLBackendUtils.constructJobsTableName(queueName, shard.getKey(), job.getPriority());
      stmt = conn.prepareStatement(
          String.format(MySQLQueries.ENQUEUE_INSERT, jobsTableName),
          Statement.RETURN_GENERATED_KEYS);
      stmt.setInt(1, PinLaterJobState.PENDING.getValue());
      stmt.setInt(2, job.getNumAttemptsAllowed());
      stmt.setInt(3, job.getNumAttemptsAllowed());
      stmt.setString(4, job.getCustomStatus());
      stmt.setTimestamp(5, new Timestamp(currentTimeMillis));
      stmt.setTimestamp(6, new Timestamp(job.isSetRunAfterTimestampMillis()
                                         ? job.getRunAfterTimestampMillis() : currentTimeMillis));
      stmt.setBytes(7, job.getBody());
      stmt.executeUpdate();
      rs = stmt.getGeneratedKeys();
      rs.next();
      return new PinLaterJobDescriptor(
          queueName, shard.getKey(), job.getPriority(), rs.getLong(1)).toString();
    } catch (SQLException e) {
      boolean shouldRetry = checkExceptionIsRetriable(e, shard.getKey(), "enqueue");
      if (shouldRetry && numAutoRetries > 0) {
        // Retry the enqueue, potentially on a different shard.
        Stats.incr("enqueue-failures-retry");
        return enqueueSingleJob(queueName, job, numAutoRetries - 1);
      }
      // Out of retries, throw the exception. Wrap it into a PinLaterException if the exception
      // is recognized and return the appropriate error code.
      if (MySQLBackendUtils.isDatabaseDoesNotExistException(e)) {
        throw new PinLaterException(ErrorCode.QUEUE_NOT_FOUND, "Queue not found: " + queueName);
      }
      throw e;
    } finally {
      JdbcUtils.closeResultSet(rs);
      JdbcUtils.closeStatement(stmt);
      JdbcUtils.closeConnection(conn);
    }
  }

  @Override
  protected PinLaterDequeueResponse dequeueJobsFromShard(
      final String queueName,
      final String shardName,
      final int priority,
      String claimDescriptor,
      int jobsNeeded,
      int numAutoRetries,
      boolean dryRun) throws IOException, SQLException, PinLaterException {
    String jobsTableName = MySQLBackendUtils.constructJobsTableName(queueName, shardName, priority);
    Connection conn = null;
    PinLaterDequeueResponse shardResponse = new PinLaterDequeueResponse();
    final long currentTimeMillis = System.currentTimeMillis();

    if (!mySQLHealthMonitor.isHealthy(shardName)) {
      LOG.debug("Skipping unhealthy shard on dequeue: " + shardName);
      Stats.incr("mysql-unhealthy-shard-dequeue");
      return shardResponse;
    }

    try {
      ImmutableMap<String, MySQLDataSources> shardMap = shardMapRef.get();
      conn = shardMap.get(shardName).getGeneralDataSource().getConnection();
      RowProcessor<Tuple6<String, Integer, Integer, Timestamp, Timestamp, ByteBuffer>>
          dequeueRowProcessor = constructDequeueRowProcessor(queueName, shardName, priority);

      if (dryRun) {
        // If this is a dry run, just retrieve the relevant pending jobs.
        List<Tuple6<String, Integer, Integer, Timestamp, Timestamp, ByteBuffer>> resultRows =
            JdbcUtils.select(
                conn,
                String.format(MySQLQueries.DEQUEUE_DRY_RUN_SELECT, jobsTableName),
                dequeueRowProcessor,
                PinLaterJobState.PENDING.getValue(),
                jobsNeeded);
        shardResponse = convertResultsIntoDequeueResponse(resultRows);
      } else {
        // If not a dry run, then we'll want to actually update the job state and claimDescriptor.
        int rowsUpdated = JdbcUtils.executeUpdate(
            conn,
            String.format(MySQLQueries.DEQUEUE_UPDATE, jobsTableName),
            claimDescriptor,
            PinLaterJobState.IN_PROGRESS.getValue(),
            PinLaterJobState.PENDING.getValue(),
            jobsNeeded);

        if (rowsUpdated > 0) {
          List<Tuple6<String, Integer, Integer, Timestamp, Timestamp, ByteBuffer>> resultRows =
              JdbcUtils.select(
                  conn,
                  String.format(MySQLQueries.DEQUEUE_SELECT, jobsTableName),
                  dequeueRowProcessor,
                  claimDescriptor);
          for (Tuple6<String, Integer, Integer, Timestamp, Timestamp,
              ByteBuffer> tuple : resultRows) {
            int attemptsAllowed = tuple._2();
            int attemptsRemaining = tuple._3();
            long updatedAtMillis = tuple._4().getTime();
            long createdAtMillis = tuple._5().getTime();
            if (attemptsAllowed == attemptsRemaining) {
              Stats.addMetric(String.format("%s_first_dequeue_delay_ms", queueName),
                  (int) (currentTimeMillis - createdAtMillis));
            }
            Stats.addMetric(String.format("%s_dequeue_delay_ms", queueName),
                (int) (currentTimeMillis - updatedAtMillis));
          }
          shardResponse = convertResultsIntoDequeueResponse(resultRows);
        }
      }

      mySQLHealthMonitor.recordSample(shardName, true);
    } catch (SQLException e) {
      mySQLHealthMonitor.recordSample(shardName, false);
      boolean shouldRetry = checkExceptionIsRetriable(e, shardName, "dequeue");
      if (shouldRetry && numAutoRetries > 0) {
        // Retry on the same shard.
        Stats.incr("dequeue-failures-retry");
        return dequeueJobsFromShard(queueName, shardName, priority, claimDescriptor,
            jobsNeeded, numAutoRetries - 1, dryRun);
      }
      // Out of retries, throw the exception. Wrap it into a PinLaterException if the exception
      // is recognized and return the appropriate error code.
      if (MySQLBackendUtils.isDatabaseDoesNotExistException(e)) {
        throw new PinLaterException(ErrorCode.QUEUE_NOT_FOUND, "Queue not found: " + queueName);
      }
      throw e;
    } finally {
      JdbcUtils.closeConnection(conn);
    }

    return shardResponse;
  }

  @Override
  protected void ackSingleJob(final String queueName,
                              boolean succeeded,
                              PinLaterJobAckInfo jobAckInfo,
                              int numAutoRetries)
      throws SQLException, PinLaterException {
    PinLaterJobDescriptor jobDesc = new PinLaterJobDescriptor(jobAckInfo.getJobDescriptor());
    String jobsTableName = MySQLBackendUtils.constructJobsTableName(
        queueName,
        jobDesc.getShardName(),
        jobDesc.getPriority());
    Connection conn = null;
    try {
      ImmutableMap<String, MySQLDataSources> shardMap = shardMapRef.get();
      conn = shardMap.get(jobDesc.getShardName()).getGeneralDataSource().getConnection();

      if (succeeded) {
        // Handle succeeded job: change state to succeeded and append custom status.
        JdbcUtils.executeUpdate(
            conn,
            String.format(MySQLQueries.ACK_SUCCEEDED_UPDATE, jobsTableName),
            PinLaterJobState.SUCCEEDED.getValue(),
            jobAckInfo.getAppendCustomStatus(),
            jobDesc.getLocalId());
      } else {
        // Handle failed job. Depending on whether the job has attempts remaining, we need
        // to either move it to pending or failed, and append custom status either way.
        // We do this by running two queries, first the 'failed done' one and then the
        // 'failed retry', so that the appropriate status change happens.
        JdbcUtils.executeUpdate(
            conn,
            String.format(MySQLQueries.ACK_FAILED_DONE_UPDATE, jobsTableName),
            PinLaterJobState.FAILED.getValue(),
            jobAckInfo.getAppendCustomStatus(),
            jobDesc.getLocalId(),
            PinLaterJobState.IN_PROGRESS.getValue());
        JdbcUtils.executeUpdate(
            conn,
            String.format(MySQLQueries.ACK_FAILED_RETRY_UPDATE, jobsTableName),
            PinLaterJobState.PENDING.getValue(),
            jobAckInfo.getAppendCustomStatus(),
            new Timestamp(System.currentTimeMillis() + jobAckInfo.getRetryDelayMillis()),
            jobDesc.getLocalId(),
            PinLaterJobState.IN_PROGRESS.getValue());
      }
    } catch (SQLException e) {
      boolean shouldRetry = checkExceptionIsRetriable(e, jobDesc.getShardName(), "ack");
      if (shouldRetry && numAutoRetries > 0) {
        // Retry on the same shard.
        Stats.incr("ack-failures-retry");
        ackSingleJob(queueName, succeeded, jobAckInfo, numAutoRetries - 1);
        return;
      }
      // Out of retries, throw the exception. Wrap it into a PinLaterException if the exception
      // is recognized and return the appropriate error code.
      if (MySQLBackendUtils.isDatabaseDoesNotExistException(e)) {
        throw new PinLaterException(ErrorCode.QUEUE_NOT_FOUND, "Queue not found: " + queueName);
      }
      throw e;
    } finally {
      JdbcUtils.closeConnection(conn);
    }
  }

  @Override
  protected void checkpointSingleJob(final String source,
                                     final String queueName,
                                     PinLaterCheckpointJobRequest request,
                                     int numAutoRetries)
      throws SQLException, PinLaterException {
    PinLaterJobDescriptor jobDesc = new PinLaterJobDescriptor(request.getJobDescriptor());
    String jobsTableName = MySQLBackendUtils.constructJobsTableName(
        queueName,
        jobDesc.getShardName(),
        jobDesc.getPriority());
    PinLaterJobState stateToSet =
        request.isMoveToPending() ? PinLaterJobState.PENDING : PinLaterJobState.IN_PROGRESS;
    Timestamp timestampToSet = request.isSetRunAfterTimestampMillis()
                               ? new Timestamp(request.getRunAfterTimestampMillis()) :
                               new Timestamp(System.currentTimeMillis());
    String sourcePatternMatcher = "%" + source + "%";

    StringBuilder queryBuilder = new StringBuilder(MySQLQueries.CHECKPOINT_JOB_HEADER);
    List<Object> args = new ArrayList<Object>();
    args.add(stateToSet.getValue());
    args.add(timestampToSet);

    if (request.isSetNewBody()) {
      queryBuilder.append(MySQLQueries.CHECKPOINT_JOB_SET_BODY);
      args.add(request.getNewBody());
    }
    if (request.isSetNumOfAttemptsAllowed()) {
      queryBuilder.append(MySQLQueries.CHECKPOINT_JOB_RESET_ATTEMPTS);
      args.add(request.getNumOfAttemptsAllowed());
      args.add(request.getNumOfAttemptsAllowed());
    }
    if (request.isSetPrependCustomStatus()) {
      queryBuilder.append(MySQLQueries.CHECKPOINT_JOB_APPEND_CUSTOM_STATUS);
      args.add(request.getPrependCustomStatus());
    }
    if (request.isMoveToPending()) {
      queryBuilder.append(MySQLQueries.CHECKPOINT_JOB_RESET_CLAIM_DESCRIPTOR);
    }

    queryBuilder.append(MySQLQueries.CHECKPOINT_JOB_FOOTER);
    String query = queryBuilder.toString();
    args.add(jobDesc.getLocalId());
    args.add(PinLaterJobState.IN_PROGRESS.getValue());
    args.add(sourcePatternMatcher);

    int numRowsUpdated = 0;
    Connection conn = null;
    try {
      conn = shardMapRef.get().get(jobDesc.getShardName()).getGeneralDataSource().getConnection();
      numRowsUpdated = JdbcUtils.executeUpdate(
          conn,
          String.format(query, jobsTableName),
          args.toArray());
    } catch (SQLException e) {
      boolean shouldRetry = checkExceptionIsRetriable(e, jobDesc.getShardName(), "checkpoint");
      if (shouldRetry && numAutoRetries > 0) {
        // Retry checkpointing the job.
        Stats.incr("checkpoint-failures-retry");
        checkpointSingleJob(source, queueName, request, numAutoRetries - 1);
        return;
      }
      // Out of retries, throw the exception. Wrap it into a PinLaterException if the exception
      // is recognized and return the appropriate error code.
      if (MySQLBackendUtils.isDatabaseDoesNotExistException(e)) {
        throw new PinLaterException(ErrorCode.QUEUE_NOT_FOUND, "Queue not found: " + queueName);
      }
      throw e;
    } finally {
      JdbcUtils.closeConnection(conn);
    }

    // If checkpointing didn't update any rows, log and record the discrepancy.
    if (numRowsUpdated == 0) {
      LOG.info("Checkpoint request was treated as a no-op from source: {}. Request: {}",
          source, request);
      Stats.incr(queueName + "_checkpoint_noop");
    }
  }

  private ImmutableMap.Entry<String, MySQLDataSources> getRandomShard() {
    ImmutableMap<String, MySQLDataSources> shardMap = shardMapRef.get();
    if (shardMap == null || shardMap.isEmpty()) {
      return null;
    }
    int rand = RANDOM.nextInt(shardMap.size());
    return shardMap.entrySet().asList().get(rand);
  }

  private ImmutableMap.Entry<String, MySQLDataSources> getRandomEnqueueableShard() {
    ImmutableMap<String, MySQLDataSources> enqueueableShardMap = getEnqueueableShards();
    if (enqueueableShardMap.isEmpty()) {
      return null;
    }
    int rand = RANDOM.nextInt(enqueueableShardMap.size());
    return enqueueableShardMap.entrySet().asList().get(rand);
  }

  /**
   * Processor for dequeue results. Converts a row into a tuple containing fields in the
   * following order: local_id, attempts_allowed, attempts_remaining, updated_at, create_at, body.
   */
  private RowProcessor<Tuple6<String, Integer, Integer, Timestamp, Timestamp, ByteBuffer>>
  constructDequeueRowProcessor(
      final String queueName,
      final String shardName,
      final int priority) {
    return new RowProcessor<Tuple6<String, Integer, Integer, Timestamp, Timestamp, ByteBuffer>>() {
      @Override
      public Tuple6<String, Integer, Integer, Timestamp, Timestamp, ByteBuffer> process(
          ResultSet rs) throws IOException, SQLException {
        return new Tuple6<String, Integer, Integer, Timestamp, Timestamp, ByteBuffer>(
            new PinLaterJobDescriptor(
                queueName,
                shardName,
                priority,
                rs.getLong(1)).toString(),
            rs.getInt(2),
            rs.getInt(3),
            rs.getTimestamp(4),
            rs.getTimestamp(5),
            ByteBuffer.wrap(rs.getBytes(6)));
      }
    };
  }

  private PinLaterDequeueResponse convertResultsIntoDequeueResponse(
      List<Tuple6<String, Integer, Integer, Timestamp, Timestamp, ByteBuffer>> resultRows) {
    PinLaterDequeueResponse shardResponse = new PinLaterDequeueResponse();

    for (Tuple6<String, Integer, Integer, Timestamp, Timestamp, ByteBuffer> tuple : resultRows) {
      String jobDescriptor = tuple._1();
      Integer attemptsAllowed = tuple._2();
      Integer attemptsRemaining = tuple._3();
      ByteBuffer body = tuple._6();

      shardResponse.putToJobs(jobDescriptor, body);

      PinLaterDequeueMetadata metadata = new PinLaterDequeueMetadata();
      metadata.setAttemptsAllowed(attemptsAllowed);
      metadata.setAttemptsRemaining(attemptsRemaining);
      shardResponse.putToJobMetadata(jobDescriptor, metadata);
    }

    return shardResponse;
  }

  /**
   * Returns an empty string if the incoming bodyRegexTomatch string is null or empty; otherwise it
   * returns the MySQL clause that will retrieve jobs with bodies matching this regex string.
   */
  private String getBodyRegexClause(String bodyRegexTomatch) {
    if (Strings.isNullOrEmpty(bodyRegexTomatch)) {
      return "";
    } else {
      return String.format(MySQLQueries.BODY_REGEX_CLAUSE, bodyRegexTomatch);
    }
  }

  /**
   * Checks the incoming SQLException and returns true if the exception is one that indicates
   * a particular request can be retried. Also collects relevant stats.
   */
  private boolean checkExceptionIsRetriable(SQLException e, String shardName, String methodName) {
    if (MySQLBackendUtils.isExceptionIndicativeOfOverload(e)) {
      // If the shard seems overloaded, we simply return an empty response
      // so the caller can skip to the next shard.
      LOG.debug("MySQL shard {} seems overloaded:", shardName, e);
      Stats.incr("mysql-overload-" + methodName);
      return true;
    } else if (MySQLBackendUtils.isDeadlockException(e)) {
      // Deadlocks are occasionally expected for our high-contention queries.
      // In this case, we retry on the same shard.
      Stats.incr("mysql-deadlock-" + methodName);
      return true;
    } else if (MySQLBackendUtils.isDatabaseDoesNotExistException(e)) {
      Stats.incr("mysql-queue-not-found-" + methodName);
      return false;
    }
    return false;
  }
}
