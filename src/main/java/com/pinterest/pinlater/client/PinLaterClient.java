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
package com.pinterest.pinlater.client;

import com.pinterest.pinlater.thrift.PinLater;

import com.twitter.common.zookeeper.ServerSet;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster;
import com.twitter.util.Duration;
import org.apache.commons.cli.CommandLine;
import org.apache.thrift.protocol.TBinaryProtocol;
import scala.Option;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Class to hold the service and iface for a PinLater client.
 */
public class PinLaterClient {

  private final Service<ThriftClientRequest, byte[]> service;
  private final PinLater.ServiceIface iface;

  /**
   * Returns instance of PinLaterClient that creates connection that is not stored in the client
   * connection pool.
   * @param cmdLine
   */
  public PinLaterClient(CommandLine cmdLine) {
    this(
        cmdLine.getOptionValue("host"),
        Integer.parseInt(cmdLine.getOptionValue("port")),
        Integer.parseInt(cmdLine.getOptionValue("concurrency", "1")));
  }

  public PinLaterClient(String host, int port, int concurrency) {
    this.service = ClientBuilder.safeBuild(
        ClientBuilder.get()
            .hosts(new InetSocketAddress(host, port))
            .codec(ThriftClientFramedCodec.apply(Option.apply(new ClientId("pinlaterclient"))))
            .hostConnectionLimit(concurrency)
            .tcpConnectTimeout(Duration.apply(2, TimeUnit.SECONDS))
            .requestTimeout(Duration.apply(10, TimeUnit.SECONDS))
            .retries(1));
    this.iface = new PinLater.ServiceToClient(service, new TBinaryProtocol.Factory());
  }

  public PinLaterClient(ServerSet serverSet, int concurrency) {
    ZookeeperServerSetCluster cluster = new ZookeeperServerSetCluster(serverSet);
    ClientBuilder builder = ClientBuilder.get().cluster(cluster);
    this.service = ClientBuilder.safeBuild(
        builder.codec(ThriftClientFramedCodec.get())
            .tcpConnectTimeout(Duration.apply(2, TimeUnit.SECONDS))
            .requestTimeout(Duration.apply(10, TimeUnit.SECONDS))
            .hostConnectionLimit(concurrency));
    this.iface = new PinLater.ServiceToClient(service, new TBinaryProtocol.Factory());
  }

  public Service<ThriftClientRequest, byte[]> getService() {
    return service;
  }

  public PinLater.ServiceIface getIface() {
    return iface;
  }
}
