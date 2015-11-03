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

import com.pinterest.pinlater.backends.common.BackendQueueMonitorBaseTest;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import java.io.InputStream;

public class MySQLQueueMonitorTest extends BackendQueueMonitorBaseTest<PinLaterMySQLBackend,
    MySQLQueueMonitor> {

  private static final String QUEUE_NAME = "mysql_queue_monitor_test";

  private PinLaterMySQLBackend backend;
  private PropertiesConfiguration configuration;

  @Override
  protected String getQueueName() {
    return QUEUE_NAME;
  }

  @Override
  protected PinLaterMySQLBackend getBackend() {
    return backend;
  }

  @Override
  protected MySQLQueueMonitor createQueueMonitor(long jobClaimedTimeoutMillis,
                                                 long jobSucceededGCTimeoutMillis,
                                                 long jobFailedGCTimeoutMillis) {
    InputStream mysqlConfigStream = ClassLoader.getSystemResourceAsStream("mysql.local.json");
    ImmutableMap<String, MySQLDataSources> shardMap =
        MySQLBackendUtils.buildShardMap(mysqlConfigStream, configuration);
    return new MySQLQueueMonitor(shardMap,
        1000,  // update max size
        3,     // auto retries
        1,     // log interval
        jobClaimedTimeoutMillis,
        jobSucceededGCTimeoutMillis,
        jobFailedGCTimeoutMillis,
        3      // max queue priority
    );
  }

  @Before
  public void beforeTest() throws Exception {
    // If there is no local MySQL, skip this test.
    boolean isLocalMysqlRunning = LocalMySQLChecker.isRunning();
    Assume.assumeTrue(isLocalMysqlRunning);

    configuration = new PropertiesConfiguration();
    try {
      configuration.load(ClassLoader.getSystemResourceAsStream("pinlater.test.properties"));
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
    System.setProperty("backend_config", "mysql.local.json");

    backend = new PinLaterMySQLBackend(
        configuration, "localhost", System.currentTimeMillis());

    // Delete the test queue, if it exists already, for some reason.
    backend.deleteQueue(QUEUE_NAME).get();

    // Now create it.
    backend.createQueue(QUEUE_NAME).get();
  }

  @After
  public void afterTest() {
    boolean isLocalMysqlRunning = LocalMySQLChecker.isRunning();
    if (!isLocalMysqlRunning) {
      return;
    }

    // Clean up the test queue.
    backend.deleteQueue(QUEUE_NAME).get();
  }
}
