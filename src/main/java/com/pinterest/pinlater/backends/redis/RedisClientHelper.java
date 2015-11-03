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

/**
 * RedisClientBuilders abstract away redis client's from RedisPools.
 *
 * RedisClientBuilders MUST be able to handle concurrent requests.
 *
 * @param <RC> RedisClient type, for example JedisPool
 */
public interface RedisClientHelper<RC> {

  /**
   * createClient should create a new client of type RC (Redis Client)
   * @param redisClientConfig A RedisClientConfig representing the parameters the client utilize
   * @param endPoint The host that this client should connect to
   * @return A client adhering to redisClientConfig connected to endPoint
   */
  RC createClient(RedisClientConfig redisClientConfig, EndPoint endPoint);

  /**
   * destroyClient should cleanup the redis client
   * @param redisClient The client to be cleaned up.
   */
  void destroyClient(RC redisClient);

  /**
   * clientPing should ping the redis instance connected to this client.
   *
   * clientPing is used in the RedisHeartBeater to ensure redis instance liveliness.
   *
   * @param redisClient The redisClient to use to ping redis
   * @return true if ping was successful and replied PONG, false otherwise
   */
  boolean clientPing(RC redisClient);
}
