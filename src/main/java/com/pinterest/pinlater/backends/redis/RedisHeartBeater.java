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
import com.pinterest.pinlater.commons.healthcheck.HeartBeater;


public class RedisHeartBeater<RC> implements HeartBeater {

  private final RC redisClient;
  private final RedisClientHelper<RC> redisClientHelper;

  public RedisHeartBeater(RedisClientHelper redisClientHelper, RC redisClient) {
    this.redisClient = Preconditions.checkNotNull(redisClient);
    this.redisClientHelper = redisClientHelper;
  }

  @Override
  public boolean ping() throws Exception {
    return redisClientHelper.clientPing(redisClient);
  }
}
