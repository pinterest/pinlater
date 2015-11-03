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

import com.pinterest.pinlater.backends.mysql.PinLaterMySQLBackend;
import com.pinterest.pinlater.backends.redis.PinLaterRedisBackend;
import com.pinterest.pinlater.commons.ostrich.OstrichAdminService;
import com.pinterest.pinlater.commons.serviceframework.ServiceShutdownHook;
import com.pinterest.pinlater.thrift.PinLater;

import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.stats.OstrichStatsReceiver;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Duration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Main class for PinLaterServer.
 */
public class PinLaterServer {

  private static final Logger LOG = LoggerFactory.getLogger(PinLaterServer.class);
  private static final PropertiesConfiguration CONFIGURATION = new PropertiesConfiguration();
  private static final long SERVER_START_TIME_MILLIS = System.currentTimeMillis();

  static {
    try {
      CONFIGURATION.load(ClassLoader.getSystemResourceAsStream(
          System.getProperty("server_config")));
    } catch (ConfigurationException e) {
      LOG.error("Failed to load server configuration", e);
      throw new RuntimeException(e);
    }
  }

  private static PinLaterBackendIface getBackendIface(String backend, String serverHostName)
      throws Exception {
    InputStream backendConfigStream = ClassLoader.getSystemResourceAsStream(
        System.getProperty("backend_config"));
    if (backend != null && backend.equals("redis")) {
      return new PinLaterRedisBackend(
          CONFIGURATION, backendConfigStream, serverHostName, SERVER_START_TIME_MILLIS);
    } else {
      return new PinLaterMySQLBackend(
          CONFIGURATION, serverHostName, SERVER_START_TIME_MILLIS);
    }
  }

  public static void main(String[] args) {
    try {
      String serverHostName = InetAddress.getLocalHost().getHostName();
      PinLaterQueueConfig queueConfig = new PinLaterQueueConfig(CONFIGURATION);
      queueConfig.initialize();
      String backend = CONFIGURATION.getString("PINLATER_BACKEND");
      PinLaterBackendIface backendIFace = getBackendIface(backend, serverHostName);
      PinLaterServiceImpl serviceImpl = new PinLaterServiceImpl(backendIFace, queueConfig);
      PinLater.Service service = new PinLater.Service(serviceImpl, new TBinaryProtocol.Factory());
      ServiceShutdownHook.register(ServerBuilder.safeBuild(
          service,
          ServerBuilder.get()
              .name("PinLaterService")
              .codec(ThriftServerFramedCodec.get())
              .hostConnectionMaxIdleTime(Duration.fromTimeUnit(
                  CONFIGURATION.getInt("SERVER_CONN_MAX_IDLE_TIME_MINUTES"), TimeUnit.MINUTES))
              .maxConcurrentRequests(CONFIGURATION.getInt("MAX_CONCURRENT_REQUESTS"))
              .reportTo(new OstrichStatsReceiver(Stats.get("")))
              .bindTo(new InetSocketAddress(CONFIGURATION.getInt("THRIFT_PORT")))));
      new OstrichAdminService(CONFIGURATION.getInt("OSTRICH_PORT")).start();

      LOG.info("\n#######################################"
          + "\n#      Ready To Serve Requests.       #"
          + "\n#######################################");
    } catch (Exception e) {
      LOG.error("Failed to start the pinlater server", e);
      System.exit(1);
    }
  }
}
