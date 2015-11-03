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
package com.pinterest.pinlater.commons.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * An utility class to make it easy to retrieve a single column from the ResultSet.
 * @param <T> the return type of the column to be processed.
 */
public class SingleColumnRowProcessor<T> implements RowProcessor<T> {

  private Class<T> mExpectedType;
  private int mIndex;

  /**
   * Constructor.
   *
   * @param expectedType the class of the expected return type of the target column in ResultSet.
   */
  public SingleColumnRowProcessor(Class<T> expectedType) {
    this(expectedType, 1);
  }

  /**
   * Constructor. Use this constructor if the target column is not the first column in the
   * rows of the ResultSet.
   *
   * @param expectedType the class of the expected return type of the target column in ResultSet.
   * @param index the index of the column in the ResultSet.
   */
  public SingleColumnRowProcessor(Class<T> expectedType, int index) {
    this.mExpectedType = expectedType;
    this.mIndex = index;
  }

  /**
   * The actual implementation of retrieving a column from the current row in the ResultSet.
   *
   * @param rs the ResultSet from which to process the current row.
   * @return the corresponding value from processing the current row in the ResultSet.
   * @throws SQLException
   */
  public T process(ResultSet rs) throws SQLException {
    // Extract column value from JDBC ResultSet.
    return (T) JdbcUtils.getResultSetValue(rs, this.mIndex, this.mExpectedType);
  }

}
