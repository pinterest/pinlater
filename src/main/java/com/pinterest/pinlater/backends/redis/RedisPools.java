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
package com.pinterest.pinlater.backends.redis;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.concurrent.TimeUnit;

/**
 * Helper class used by the PinLaterRedisBackend to encapsulate Redis pools.
 */
public class RedisPools {

  private static final Logger LOG = LoggerFactory.getLogger(RedisPools.class);

  private final String host;
  private final int port;
  private final boolean dequeueOnly;
  private final JedisPool generalRedisPool;
  private final JedisPool monitorRedisPool;

  public RedisPools(PropertiesConfiguration configuration, String host, int port,
                    boolean dequeueOnly) {
    int numGeneralConnections = configuration.getInt("BACKEND_CONNECTIONS_PER_SHARD");
    int generalSocketTimeoutMillis = (int) TimeUnit.SECONDS.toMillis(
        configuration.getInt("BACKEND_SOCKET_TIMEOUT_SECONDS"));
    int numMonitorConnections = configuration.getInt("MONITOR_CONNECTIONS_PER_SHARD", 3);
    int monitorSocketTimeoutMillis = (int) TimeUnit.SECONDS.toMillis(
        configuration.getInt("MONITOR_SOCKET_TIMEOUT_SECONDS", 10));
    int maxWaitMillis = configuration.getInt("BACKEND_CONNECTION_MAX_WAIT_MILLIS");
    this.host = host;
    this.port = port;
    this.dequeueOnly = dequeueOnly;
    this.generalRedisPool = createRedisPool(
        host, port, numGeneralConnections,
        maxWaitMillis, generalSocketTimeoutMillis);
    this.monitorRedisPool = createRedisPool(
        host, port, numMonitorConnections,
        maxWaitMillis, monitorSocketTimeoutMillis);
  }

  public String getHost() { return host; }

  public int getPort() { return port; }

  public boolean getDequeueOnly() { return dequeueOnly; }

  public JedisPool getGeneralRedisPool() { return generalRedisPool; }

  public JedisPool getMonitorRedisPool() { return monitorRedisPool; }

  private static JedisPool createRedisPool(
      String host, int port, int poolSize, int maxWaitMillis, int socketTimeoutMillis) {
    JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxWait(maxWaitMillis);
    config.setMaxActive(poolSize);
    config.setMaxIdle(poolSize);
    // Deal with idle connection eviction.
    config.setTestOnBorrow(false);
    config.setTestOnReturn(false);
    config.setTestWhileIdle(true);
    config.setMinEvictableIdleTimeMillis(5 * 60 * 1000);
    config.setTimeBetweenEvictionRunsMillis(3 * 60 * 1000);
    config.setNumTestsPerEvictionRun(poolSize);

    JedisPool pool = new JedisPool(config, host, port, socketTimeoutMillis);
    // Force connection pool initialization.
    Jedis jedis = null;
    try {
      jedis = pool.getResource();
    } catch (JedisConnectionException e) {
      LOG.error(
          String.format("Failed to get a redis connection when creating redis pool, "
              + "host: %s, port: %d", host, port),
          e);
    } finally {
      pool.returnResource(jedis);
    }
    return pool;
  }
}
