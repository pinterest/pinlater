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

import com.pinterest.pinlater.commons.jdbc.JdbcUtils;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;

/**
 * Helper class used by the PinLaterMySQLBackend to encapsulate JDBC data sources.
 */
public class MySQLDataSources {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLDataSources.class);

  private final DataSource generalDataSource;
  private final DataSource monitorDataSource;
  private final String host;
  private final int port;
  private final String user;
  private final String passwd;
  private final AtomicBoolean dequeueOnly;

  public MySQLDataSources(PropertiesConfiguration configuration, String host, int port,
                          String user, String passwd, boolean dequeueOnly) {
    int numGeneralConnections = configuration.getInt("BACKEND_CONNECTIONS_PER_SHARD");
    int generalSocketTimeoutMillis = (int) TimeUnit.SECONDS.toMillis(
        configuration.getInt("BACKEND_SOCKET_TIMEOUT_SECONDS"));
    int numMonitorConnections = configuration.getInt("MONITOR_CONNECTIONS_PER_SHARD", 3);
    int monitorSocketTimeoutMillis = (int) TimeUnit.SECONDS.toMillis(
        configuration.getInt("MONITOR_SOCKET_TIMEOUT_SECONDS", 10));
    int maxWaitMillis = configuration.getInt("BACKEND_CONNECTION_MAX_WAIT_MILLIS");
    this.generalDataSource = createDataSource(
        host, port, user, passwd, numGeneralConnections,
        maxWaitMillis, generalSocketTimeoutMillis);
    this.monitorDataSource = createDataSource(
        host, port, user, passwd, numMonitorConnections,
        maxWaitMillis, monitorSocketTimeoutMillis);
    this.host = host;
    this.port = port;
    this.user = user;
    this.passwd = passwd;
    this.dequeueOnly = new AtomicBoolean(dequeueOnly);
  }

  public DataSource getGeneralDataSource() { return generalDataSource; }

  public DataSource getMonitorDataSource() { return monitorDataSource; }

  public boolean isDequeueOnly() { return dequeueOnly.get(); }

  public void setDequeueOnly(Boolean dequeueOnly) { this.dequeueOnly.set(dequeueOnly); }

  // NOTE: We don't check dequeueOnly here because it should be configurable while maintaining
  // the connection.
  public boolean needNewConnection(String host, int port, String user, String passwd) {
    return this.host.equals(host) && this.port == port
        && this.user.equals(user) && this.passwd.equals(passwd);
  }

  // NOTE: We intentionally clone this method from DataSourceUtil, since we want to carefully
  // control data source and ConnectorJ configuration.
  private static DataSource createDataSource(
      String host, int port, String user, String passwd, int poolSize,
      int maxWaitMillis, int socketTimeoutMillis) {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setDriverClassName("com.mysql.jdbc.Driver");
    dataSource.setUrl(String.format(
        "jdbc:mysql://%s:%d?"
            + "connectTimeout=5000&"
            + "socketTimeout=%d&"
            + "enableQueryTimeouts=false&"
            + "cachePrepStmts=true&"
            + "characterEncoding=UTF-8",
        host,
        port,
        socketTimeoutMillis));
    dataSource.setUsername(user);
    dataSource.setPassword(passwd);
    dataSource.setDefaultAutoCommit(true);
    dataSource.setInitialSize(poolSize);
    dataSource.setMaxActive(poolSize);
    dataSource.setMaxIdle(poolSize);
    // deal with idle connection eviction
    dataSource.setValidationQuery("SELECT 1 FROM DUAL");
    dataSource.setTestOnBorrow(false);
    dataSource.setTestOnReturn(false);
    dataSource.setTestWhileIdle(true);
    dataSource.setMinEvictableIdleTimeMillis(5 * 60 * 1000);
    dataSource.setTimeBetweenEvictionRunsMillis(3 * 60 * 1000);
    dataSource.setNumTestsPerEvictionRun(poolSize);
    // max wait in milliseconds for a connection.
    dataSource.setMaxWait(maxWaitMillis);
    // force connection pool initialization.
    Connection conn = null;
    try {
      // Here not getting the connection from ThreadLocal no need to worry about that.
      conn = dataSource.getConnection();
    } catch (SQLException e) {
      LOG.error(
          String.format("Failed to get a mysql connection when creating DataSource, "
              + "host: %s, port: %d", host, port),
          e);
    } finally {
      JdbcUtils.closeConnection(conn);
    }
    return dataSource;
  }
}
