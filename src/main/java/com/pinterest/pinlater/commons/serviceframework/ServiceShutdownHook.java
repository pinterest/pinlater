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
package com.pinterest.pinlater.commons.serviceframework;

import com.twitter.finagle.builder.Server;
import com.twitter.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a utility to gracefully shutdown your finagle server. Upon jvm being shutdown, it
 * first stops the server from accepting new connections, and then wait till either the grace
 * period ends or all the requests drained with all the existing established connections, whichever
 * is earlier.
 */
public class ServiceShutdownHook {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceShutdownHook.class);

  // default grace period is 10 seconds
  public static final long DEFAULT_GRACE_PERIOD_MILLIS = 10000L;

  /**
   * Turn on graceful shutdown on the finagle server passed in with the grace period of 10 seconds.
   *
   * @param server the finagle server instance built by finagle ServerBuilder
   */
  public static void register(final Server server) {
    register(server, Duration.fromMilliseconds(DEFAULT_GRACE_PERIOD_MILLIS));
  }

  /**
   * Turn on graceful shutdown on the finagle server passed in with the grace period passed in.
   *
   * @param server the finagle server instance built by finagle ServerBuilder.
   * @param gracePeriod the time period the shutdown process will wait for till the existing
   *                    requests drain. If the existing requests are not being drain after grace
   *                    period expires, the server will be forcefully shutdown.
   */
  public static void register(final Server server, final Duration gracePeriod) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("Try to shut down the server gracefully: {}", gracePeriod);
        server.close(gracePeriod);
        LOG.info("Finished server graceful shutdown");
      }
    });
  }
}
