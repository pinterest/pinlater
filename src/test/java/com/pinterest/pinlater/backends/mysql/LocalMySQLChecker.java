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

import com.pinterest.pinlater.commons.jdbc.JdbcUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class to check if mysql is running on localhost.
 */
public class LocalMySQLChecker {

  private static AtomicBoolean sChecked = new AtomicBoolean(false);
  private static AtomicBoolean sIsRunning = new AtomicBoolean(false);

  public static boolean isRunning() {
    if (sChecked.get()) {
      return sIsRunning.get();
    }
    synchronized (LocalMySQLChecker.class) {
      Connection conn = null;
      try {
        DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "");
        sIsRunning.set(true);
      } catch (SQLException e) {
        sIsRunning.set(false);
        JdbcUtils.closeConnection(conn);
      }
      sChecked.set(true);
      return sIsRunning.get();
    }
  }
}
