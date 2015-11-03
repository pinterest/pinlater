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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.dbcp.SQLNestedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Encapsulates static utility methods used by the PinLaterMySQLBackend and related classes.
 */
public final class MySQLBackendUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLBackendUtils.class);

  private static final int MYSQL_DB_NAME_TOO_LONG_ERROR_CODE = 1059;
  private static final int MYSQL_DB_ALREADY_EXISTS_ERROR_CODE = 1007;
  private static final int MYSQL_DB_DOESNT_EXIST_ERROR_CODE = 1146;

  /**
   * MySQL currently only supports 64 characters for certain identifiers, including database
   * names. http://dev.mysql.com/doc/refman/5.5/en/identifiers.html
   */
  public static final int MYSQL_MAX_DB_NAME_LENGTH = 64;

  public static final String PINLATER_QUEUE_DB_PREFIX = "plq";

  private MySQLBackendUtils() {}

  /**
   * Constructs the MySQL database name for a pinlater queue.
   *
   * @param queueName name of the pinlater queue.
   * @param shardName MySQL shard name.
   * @return MySQL database name.
   */
  public static String constructDBName(String queueName, String shardName) {
    return String.format("%s_%s_%s",
        PINLATER_QUEUE_DB_PREFIX, shardName, queueName);
  }

  /**
   * Constructs the shard name given a replica id and DB id
   * @param replicaId MySQL replica set id
   * @param dbId database id within a MySQL instance
   * @return shard name
   */
  public static String constructShardName(int replicaId, int dbId) {
    if (dbId == 0) {
      // NOTE: Don't append DB id when it's 0 for backward compatibility.
      return String.valueOf(replicaId);
    } else {
      return String.format("%dd%d", replicaId, dbId);
    }
  }

  /**
   * Extracts the queue name given a pinlater queue database name.
   *
   * @param dbName pinlater queue database name.
   * @return queue name.
   */
  public static String queueNameFromDBName(String dbName) {
    // NOTE: Underscores are permitted in the queue name, so we limit the number of splits.
    String[] tokens = dbName.split("_", 3);
    return tokens[2];
  }

  /**
   * Extracts the shard name given a pinlater queue database name.
   *
   * @param dbName pinlater queue database name.
   * @return shard name.
   */
  public static String shardNameFromDBName(String dbName) {
    String[] tokens = dbName.split("_");
    return tokens[1];
  }

  /**
   * Constructs the MySQL jobs table name given a queue name, shard name and priority.
   *
   * @param queueName name of the queue.
   * @param shardName MySQL shard name.
   * @param priority priority level.
   * @return MySQL jobs table name.
   */
  public static String constructJobsTableName(String queueName, String shardName, int priority) {
    return String.format("%s.jobs_p%1d", constructDBName(queueName, shardName), priority);
  }

  public static boolean isDeadlockException(Exception e) {
    return e instanceof MySQLTransactionRollbackException
        || (e instanceof BatchUpdateException
                && e.getCause() instanceof MySQLTransactionRollbackException);
  }

  /**
   * Indicates whether an exception is a symptom of MySQL being overloaded or slow.
   * Typical symptoms are connection pool exhaustion or MySQL connection/socket timeouts.
   */
  public static boolean isExceptionIndicativeOfOverload(Exception e) {
    return e instanceof MySQLNonTransientConnectionException
        || e instanceof CommunicationsException
        || (e instanceof SQLNestedException
                && e.getCause() instanceof NoSuchElementException);
  }

  /**
   * Indicates whether an exception is a result of a particular database
   * identifier being too long . Error codes for MySQL can be found here:
   * http://dev.mysql.com/doc/refman/5.6/en/error-messages-server.html
   */
  public static boolean isDatabaseNameTooLongException(Exception e) {
    return e instanceof MySQLSyntaxErrorException
        && ((MySQLSyntaxErrorException) e).getErrorCode() == MYSQL_DB_NAME_TOO_LONG_ERROR_CODE;
  }

  /**
   * Indicates whether an exception is a result of a particular database already
   * existing in the MySQL instance. Error codes for MySQL can be found here:
   * http://dev.mysql.com/doc/refman/5.6/en/error-messages-server.html
   */
  public static boolean isDatabaseAlreadyExistsException(Exception e) {
    return e instanceof SQLException
        && ((SQLException) e).getErrorCode() == MYSQL_DB_ALREADY_EXISTS_ERROR_CODE;
  }

  /**
   * Indicates whether an exception is a result of a particular database or table not
   * existing in the MySQL instance. Error codes for MySQL can be found here:
   * http://dev.mysql.com/doc/refman/5.6/en/error-messages-server.html
   */
  public static boolean isDatabaseDoesNotExistException(Exception e) {
    return e instanceof MySQLSyntaxErrorException
        && ((MySQLSyntaxErrorException) e).getErrorCode() == MYSQL_DB_DOESNT_EXIST_ERROR_CODE;
  }

  /**
   * Gets the names of all queues found at the specified MySQL instance. Note that it is the
   * callers responsibility to close the connection after this method is called.
   *
   * @param conn Connection to the MySQL instance.
   * @param shardName
   * @return Set containing queue name strings.
   */
  public static Set<String> getQueueNames(Connection conn, String shardName) throws SQLException {
    ResultSet dbNameRs = conn.getMetaData().getCatalogs();
    Set<String> queueNames = Sets.newHashSet();
    while (dbNameRs.next()) {
      String name = dbNameRs.getString("TABLE_CAT");
      if (name.startsWith(MySQLBackendUtils.PINLATER_QUEUE_DB_PREFIX)
          && shardNameFromDBName(name).equals(shardName)) {
        queueNames.add(MySQLBackendUtils.queueNameFromDBName(name));
      }
    }
    return queueNames;
  }

  /**
   * Creates the MySQL shard map from config.
   *
   * @param mysqlConfigStream InputStream containing the MySQL config json.
   * @param configuration  PropertiesConfiguration object.
   * @return A map shardName -> MySQLDataSources.
   */
  public static ImmutableMap<String, MySQLDataSources> buildShardMap(
      InputStream mysqlConfigStream,
      PropertiesConfiguration configuration) {
    MySQLConfigSchema mysqlConfig;
    try {
      mysqlConfig = MySQLConfigSchema.read(
          Preconditions.checkNotNull(mysqlConfigStream),
          configuration.getString("SHARD_ALLOWED_HOSTNAME_PREFIX"));
    } catch (Exception e) {
      LOG.error("Failed to load mysql configuration", e);
      throw new RuntimeException(e);
    }

    ImmutableMap.Builder<String, MySQLDataSources> shardMapBuilder =
        new ImmutableMap.Builder<String, MySQLDataSources>();
    int numDbPerQueue = configuration.getInt("MYSQL_NUM_DB_PER_QUEUE", 1);
    for (MySQLConfigSchema.Shard shard : mysqlConfig.shards) {
      MySQLDataSources dataSources = new MySQLDataSources(
          configuration,
          shard.shardConfig.master.host,
          shard.shardConfig.master.port,
          shard.shardConfig.user,
          shard.shardConfig.passwd,
          shard.shardConfig.dequeueOnly);
      // Share data source between databases on the same MySQL instance
      for (int dbId = 0; dbId < numDbPerQueue; dbId++) {
        shardMapBuilder.put(MySQLBackendUtils.constructShardName(shard.id, dbId), dataSources);
      }
    }
    return shardMapBuilder.build();
  }
}
