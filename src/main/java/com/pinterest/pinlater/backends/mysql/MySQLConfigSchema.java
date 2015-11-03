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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Helper class used by the PinLaterMySQLBackend that describes the MySQL config schema
 * (list of shards and the machines they map to).
 */
public class MySQLConfigSchema {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public List<Shard> shards;

  public static class Shard {

    public int id;
    public ShardConfig shardConfig;
  }

  public static class ShardConfig {

    public EndPoint master;
    public EndPoint slave;
    public String user;
    public String passwd;
    public boolean dequeueOnly = false;
  }

  public static class EndPoint {

    public String host;
    public int port;
  }

  public static MySQLConfigSchema read(InputStream json,
                                       String shardNamePrefix) throws Exception {
    JsonNode shardsJson = OBJECT_MAPPER.readTree(json);

    MySQLConfigSchema mySQLConfigSchema = new MySQLConfigSchema();
    mySQLConfigSchema.shards = new ArrayList<Shard>();

    Iterator<String> shardNameIter = shardsJson.getFieldNames();
    while (shardNameIter.hasNext()) {
      String shardName = shardNameIter.next();
      if (shardName.startsWith(shardNamePrefix)) {
        Shard shard = new Shard();
        shard.id = Integer.parseInt(shardName.replaceFirst(shardNamePrefix, ""));
        shard.shardConfig = OBJECT_MAPPER.readValue(shardsJson.get(shardName), ShardConfig.class);
        mySQLConfigSchema.shards.add(shard);
      }
    }
    return mySQLConfigSchema;
  }
}
