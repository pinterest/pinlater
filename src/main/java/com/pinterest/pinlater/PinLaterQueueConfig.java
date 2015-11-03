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
package com.pinterest.pinlater;

import com.pinterest.pinlater.commons.config.ConfigFileServerSet;
import com.pinterest.pinlater.commons.config.ConfigFileWatcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.thrift.ServiceInstance;
import com.twitter.util.ExceptionalFunction;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class that keeps track of per-queue dynamic configuration for PinLater.
 */
public class PinLaterQueueConfig {

  private static final Logger LOG = LoggerFactory.getLogger(PinLaterQueueConfig.class);

  private final AtomicReference<QueueConfigSchema> queueConfigSchemaRef =
      new AtomicReference<QueueConfigSchema>();
  private final AtomicInteger numPinLaterServers = new AtomicInteger(1);
  private final AtomicReference<ImmutableMap<String, QueueRateLimiter.IFace>> queueRateLimitMapRef =
      new AtomicReference<ImmutableMap<String, QueueRateLimiter.IFace>>();

  private final String queueConfigFilePath;
  private final String pinlaterServerSetPath;
  private final boolean pinlaterServerSetEnabled;

  public PinLaterQueueConfig(PropertiesConfiguration configuration) {
    this(configuration.getString("QUEUE_CONFIG_FILE_PATH"),
        configuration.getString("SERVER_SET_PATH"),
        configuration.getBoolean("SERVER_SET_ENABLED"));
  }

  @VisibleForTesting
  public PinLaterQueueConfig(String queueConfigFilePath,
                             String pinlaterServerSetPath,
                             boolean pinlaterServerSetEnabled) {
    this.queueConfigFilePath = queueConfigFilePath;
    this.pinlaterServerSetPath = pinlaterServerSetPath;
    this.pinlaterServerSetEnabled = pinlaterServerSetEnabled;
  }


  public void initialize() throws Exception {
    // Check if use of serverset is enabled, and if so, register a change monitor so we
    // can find out how many PinLater servers are active. We use this to compute the
    // per-server rate limit.
    if (pinlaterServerSetEnabled) {
      MorePreconditions.checkNotBlank(pinlaterServerSetPath);
      LOG.info("Monitoring pinlater serverset: {}", pinlaterServerSetPath);
      String fullServerSetPath = getClass().getResource("/" + pinlaterServerSetPath).getPath();
      ServerSet serverSet = new ConfigFileServerSet(fullServerSetPath);
      serverSet.monitor(new DynamicHostSet.HostChangeMonitor<ServiceInstance>() {
        @Override
        public void onChange(ImmutableSet<ServiceInstance> hostSet) {
          int oldSize = numPinLaterServers.get();
          int newSize = hostSet.size();
          if (newSize == 0) {
            LOG.error("PinLater serverset is empty, ignoring and keeping old size: {}", oldSize);
            return;
          }
          if (oldSize == newSize) {
            LOG.info("PinLater serverset update, size unchanged: {}", oldSize);
            return;
          }

          LOG.info("PinLater serverset update, old size: {}, new size: {}", oldSize, newSize);
          numPinLaterServers.set(newSize);
          rebuild();
        }
      });
    } else {
      LOG.info("PinLater server set is disabled; rate limits will be applied per server.");
    }

    if (queueConfigFilePath == null || queueConfigFilePath.isEmpty()) {
      LOG.info("Queue config zookeeper path not specified, using defaults.");
      return;
    }

    LOG.info("Registering watch on queue config: {}", queueConfigFilePath);
    String fullQueueConfigFilePath = getClass().getResource("/" + queueConfigFilePath).getPath();
    ConfigFileWatcher.defaultInstance().addWatch(
        fullQueueConfigFilePath, new ExceptionalFunction<byte[], Void>() {
          @Override
          public Void applyE(byte[] bytes) throws Exception {
            QueueConfigSchema queueConfigSchema = QueueConfigSchema.load(bytes);
            LOG.info("Queue config update, new value: {}", queueConfigSchema);
            queueConfigSchemaRef.set(queueConfigSchema);
            rebuild();
            return null;
          }
        });
  }

  /**
   * Determines whether a dequeue request should be allowed.
   *
   * @param queueName  name of the queue.
   * @param numJobs    number of jobs intended to be dequeued.
   * @return whether to allow the request.
   */
  public boolean allowDequeue(String queueName, int numJobs) {
    MorePreconditions.checkNotBlank(queueName);
    Preconditions.checkArgument(numJobs > 0);
    ImmutableMap<String, QueueRateLimiter.IFace> queueRateLimitMap = queueRateLimitMapRef.get();
    if (queueRateLimitMap != null && queueRateLimitMap.containsKey(queueName)) {
      return queueRateLimitMap.get(queueName).allowDequeue(numJobs);
    } else {
      // No rate limit specified for this queue, so always allow.
      return true;
    }
  }

  @VisibleForTesting
  double getDequeueRate(String queueName) {
    MorePreconditions.checkNotBlank(queueName);
    ImmutableMap<String, QueueRateLimiter.IFace> queueRateLimitMap = queueRateLimitMapRef.get();
    if (queueRateLimitMap != null && queueRateLimitMap.containsKey(queueName)) {
      return queueRateLimitMap.get(queueName).getRate();
    } else {
      // No rate limit specified for this queue.
      return Double.MAX_VALUE;
    }
  }

  /**
   * Rebuilds the per-queue configuration. This method is thread-safe.
   */
  @VisibleForTesting
  synchronized void rebuild() {
    QueueConfigSchema queueConfigSchema = queueConfigSchemaRef.get();
    int numServers = numPinLaterServers.get();

    if (queueConfigSchema == null || queueConfigSchema.queues == null
        || queueConfigSchema.queues.isEmpty()) {
      return;
    }

    ImmutableMap.Builder<String, QueueRateLimiter.IFace> builder =
        new ImmutableMap.Builder<String, QueueRateLimiter.IFace>();
    for (QueueConfigSchema.Queue queue : queueConfigSchema.queues) {
      builder.put(queue.name,
          QueueRateLimiter.create(queue.queueConfig.maxJobsPerSecond / numServers));
    }

    queueRateLimitMapRef.set(builder.build());
  }

  @VisibleForTesting
  AtomicReference<QueueConfigSchema> getQueueConfigSchemaRef() {
    return queueConfigSchemaRef;
  }

  @VisibleForTesting
  AtomicInteger getNumPinLaterServers() {
    return numPinLaterServers;
  }

  /**
   * Defines the queue configuration json schema.
   */
  public static class QueueConfigSchema {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public List<Queue> queues;

    public static class Queue {

      public String name;
      public QueueConfig queueConfig;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class QueueConfig {

      public double maxJobsPerSecond;
    }

    public static QueueConfigSchema load(byte[] bytes) throws IOException {
      return QueueConfigSchema.OBJECT_MAPPER.readValue(bytes, QueueConfigSchema.class);
    }

    @Override
    public String toString() {
      StringBuilder out = new StringBuilder();
      out.append("\nQueue Config:");
      for (Queue q : queues) {
        out.append("\nQueue: ").append(q.name);
        out.append(" maxJobsPerSecond: ").append(q.queueConfig.maxJobsPerSecond);
      }
      return out.toString();
    }
  }
}
