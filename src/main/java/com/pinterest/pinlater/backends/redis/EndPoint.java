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

public class EndPoint {

  public String host;
  public int port;
  public int socket_timeout;

  public EndPoint(String host, int port, int socket_timeout) {
    this.host = host;
    this.port = port;
    this.socket_timeout = socket_timeout;
  }

  public EndPoint(EndPoint endpoint) {
    this.host = endpoint.host;
    this.port = endpoint.port;
    this.socket_timeout = endpoint.socket_timeout;
  }

  @Override
  public String toString() {
    return "EndPoint{"
        + "host='" + host + '\''
        + ", port=" + port
        + ", socket_timeout=" + socket_timeout
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EndPoint)) {
      return false;
    }

    EndPoint endPoint = (EndPoint) o;

    if (port == endPoint.port
        && host.equals(endPoint.host)
        && socket_timeout == endPoint.socket_timeout) {
      return true;
    }

    return false;
  }

  @Override
  public int hashCode() {
    int result = (host != null) ? host.hashCode() : 0;
    result = 31 * result + port;
    result = 31 * result + socket_timeout;
    return result;
  }
}