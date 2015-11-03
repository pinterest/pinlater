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

import com.twitter.util.Function;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * JedisClientHelper creates/destroys Jedis Client Pools.
 *
 * Important parameters:
 * numConnections: Number of connections to one host.
 * socketTimeoutMillis: Socket timeout when trying to establish the new connection.
 * maxWaitMillis: If the connection pool is full, how long to wait.
 */
public class JedisClientHelper implements RedisClientHelper<JedisPool> {

  public JedisPool createClient(RedisClientConfig redisClientConfig, EndPoint endPoint) {
    JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxWait(redisClientConfig.getMaxWaitMillis());
    config.setMaxActive(redisClientConfig.getNumConnections());
    config.setMaxIdle(redisClientConfig.getNumConnections());
    // Deal with idle connection eviction.
    config.setTestOnBorrow(false);
    config.setTestOnReturn(false);
    config.setTestWhileIdle(true);
    config.setMinEvictableIdleTimeMillis(5 * 60 * 1000);
    config.setTimeBetweenEvictionRunsMillis(3 * 60 * 1000);
    config.setNumTestsPerEvictionRun(redisClientConfig.getNumConnections());
    JedisPool jedisPool = new JedisPool(
        config, endPoint.host, endPoint.port, redisClientConfig.getSocketTimeoutMillis());
    return jedisPool;
  }

  public void destroyClient(JedisPool jedisPool) {
    jedisPool.destroy();
  }

  public boolean clientPing(JedisPool jedisPool) {
    boolean result = false;
    try {
      result = RedisUtils.executeWithConnection(
          jedisPool,
          new Function<Jedis, String>() {
            @Override
            public String apply(Jedis conn) {
              return conn.ping();
            }
          }
      ).equals("PONG");
    } catch (Exception e) {
      // failed ping
    }
    return result;
  }
}
