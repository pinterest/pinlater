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

public class RedisClientConfig {

  private String serviceName;
  private int numConnections;
  private int maxWaitMillis;

  /** Timeout to setup an actual connection to Redis */
  private int socketTimeoutMillis;

  /** Timeout for a request including any replies */
  private int totalRequestTimeoutMillis;

  /** Max number of retries per request */
  private int maxRetries;

  private int redisHealthCheckConsecutiveFailures;
  private int redisHealthCheckConsecutiveSuccesses;
  private int redisHealthCheckPingIntervalSeconds;

  public RedisClientConfig() {
    this.serviceName = "redisservice";
    this.numConnections = 1000;
    this.maxWaitMillis = 250;
    this.socketTimeoutMillis = 1000;
    this.totalRequestTimeoutMillis = 1000;
    this.maxRetries = 3;
    this.redisHealthCheckConsecutiveFailures = 6;
    this.redisHealthCheckConsecutiveSuccesses = 6;
    this.redisHealthCheckPingIntervalSeconds = 5;
  }

  public String getServiceName() {
    return this.serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public int getNumConnections() {
    return this.numConnections;
  }

  public void setNumConnections(int numConnections) {
    this.numConnections = numConnections;
  }

  public int getMaxWaitMillis() {
    return this.maxWaitMillis;
  }

  public void setMaxWaitMillis(int maxWaitMillis) {
    this.maxWaitMillis = maxWaitMillis;
  }

  public int getSocketTimeoutMillis() {
    return this.socketTimeoutMillis;
  }

  public void setSocketTimeoutMillis(int socketTimeoutMillis) {
    this.socketTimeoutMillis = socketTimeoutMillis;
  }

  public int getRedisHealthCheckConsecutiveFailures() {
    return this.redisHealthCheckConsecutiveFailures;
  }

  public void setRedisHealthCheckConsecutiveFailures(int redisHealthCheckConsecutiveFailures) {
    this.redisHealthCheckConsecutiveFailures = redisHealthCheckConsecutiveFailures;
  }

  public int getRedisHealthCheckConsecutiveSuccesses() {
    return this.redisHealthCheckConsecutiveSuccesses;
  }

  public void setRedisHealthCheckConsecutiveSuccesses(int redisHealthCheckConsecutiveSuccesses) {
    this.redisHealthCheckConsecutiveSuccesses = redisHealthCheckConsecutiveSuccesses;
  }

  public int getRedisHealthCheckPingIntervalSeconds() {
    return this.redisHealthCheckPingIntervalSeconds;
  }

  public void setRedisHealthCheckPingIntervalSeconds(int redisHealthCheckPingIntervalSeconds) {
    this.redisHealthCheckPingIntervalSeconds = redisHealthCheckPingIntervalSeconds;
  }

  public int getTotalRequestTimeoutMillis() {
    return this.totalRequestTimeoutMillis;
  }

  public void setTotalRequestTimeoutMillis(int totalRequestTimeoutMillis) {
    this.totalRequestTimeoutMillis = totalRequestTimeoutMillis;
  }

  public int getMaxRetries() {
    return this.maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }
}
