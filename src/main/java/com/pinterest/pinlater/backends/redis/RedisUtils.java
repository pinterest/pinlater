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

import com.google.common.base.Preconditions;
import com.twitter.util.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RedisUtils.class);

  /**
   * Gets the connection from the connection pool and adds the wrapper catch/finally block for the
   * given function.
   *
   * This helper method saves the trouble of dealing with redis connection. When we got
   * JedisConnectionException, we will discard this connection. Otherwise, we return the connection
   * to the connection pool.
   *
   * @param jedisPool Jedis connection pool
   * @param redisDBNum Redis DB number (index) (if redisDBNum == -1, don't select a DB )
   * @param func    The function to execute inside the catch/finally block.
   * @return A Resp object, which is the return value of wrapped function.
   */
  public static <Resp> Resp executeWithConnection(JedisPool jedisPool,
                                                  int redisDBNum,
                                                  Function<Jedis, Resp> func) {
    Preconditions.checkNotNull(jedisPool);
    Preconditions.checkNotNull(func);
    Jedis conn = null;
    boolean gotJedisConnException = false;
    try {
      conn = jedisPool.getResource();
      selectRedisDB(conn, redisDBNum);
      return func.apply(conn);
    } catch (JedisConnectionException e) {
      jedisPool.returnBrokenResource(conn);
      gotJedisConnException = true;
      throw e;
    } finally {
      if (conn != null && !gotJedisConnException) {
        jedisPool.returnResource(conn);
      }
    }
  }

  /**
   * Same as above, but without the Redis DB index selection.
   *
   */
  public static <Resp> Resp executeWithConnection(JedisPool jedisPool, Function<Jedis, Resp> func)
      throws Exception {
    // NOTE: use the default DB number -1 here to indicate no DB will be selected.
    return executeWithConnection(jedisPool, -1, func);
  }

  /**
   * Select the Redis DB number (index) for the given Jedis connection.
   * If the DB number == -1, don't do anything.
   *
   * @param conn Jedis connection
   * @param redisDBNum Redis DB number (index)
   */
  private static void selectRedisDB(Jedis conn, int redisDBNum) {
    if (redisDBNum != -1) {
      conn.select(redisDBNum);
    }
  }
}
