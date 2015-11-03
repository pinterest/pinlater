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

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Helper class used by the PinLaterRedisBackend that describes the Redis config schema
 * (list of shards and the machines they map to).
 */
public class RedisConfigSchema {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public List<Shard> shards;

  public static class Shard {

    public String name;
    public ShardConfig shardConfig;
  }

  public static class ShardConfig {

    public EndPoint master;
    public EndPoint slave;
    public boolean dequeueOnly = false;
  }

  public static class EndPoint {

    public String host;
    public int port;
  }

  public static RedisConfigSchema read(InputStream json) throws IOException {
    return OBJECT_MAPPER.readValue(json, RedisConfigSchema.class);
  }
}
