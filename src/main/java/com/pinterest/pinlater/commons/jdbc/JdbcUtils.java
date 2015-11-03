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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

/**
 * JDBC Utilities.
 */
public class JdbcUtils {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);

  /**
   * close the connection.
   *
   * @param connection the connection to be closed.
   */
  public static void closeConnection(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException ex) {
        LOG.debug("Could not close JDBC Connection", ex);
      } catch (Throwable ex) {
        LOG.info("Unexpected exception on closing JDBC Connection", ex);
      }
    }
  }

  /**
   * close the statement.
   *
   * @param stmt the statement to be closed.
   */
  public static void closeStatement(Statement stmt) {
    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException ex) {
        LOG.debug("Could not close JDBC Statement", ex);
      } catch (Throwable ex) {
        LOG.info("Unexpected exception on closing JDBC Statement", ex);
      }
    }
  }

  /**
   * close the resultset.
   *
   * @param rs thre resultset to be closed.
   */
  public static void closeResultSet(ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException ex) {
        LOG.debug("Could not close JDBC ResultSet", ex);
      } catch (Throwable ex) {
        LOG.info("Unexpected exception on closing JDBC ResultSet", ex);
      }
    }
  }

  /**
   * Retrieve a JDBC column value from a ResultSet. To avoid NullPointerException, this method
   * doesn't support primitive type.
   *
   * @param rs           the result set.
   * @param index        the index of column to retrieve the value for.
   * @param expectedType the expected type for the column of the given index in the resultset.
   * @return the value in the resultset for the given column.
   * @throws SQLException
   */
  public static Object getResultSetValue(
      ResultSet rs, int index, Class expectedType) throws SQLException {
    if (expectedType == null) {
      throw new IllegalArgumentException("requiredType cannot be null");
    }

    Object value;
    boolean wasNullCheck = false;

    if (String.class.equals(expectedType)) {
      value = new String(rs.getBytes(index), Charsets.UTF_8);
    } else if (Boolean.class.equals(expectedType)) {
      value = rs.getBoolean(index);
      wasNullCheck = true;
    } else if (Byte.class.equals(expectedType)) {
      value = rs.getByte(index);
      wasNullCheck = true;
    } else if (Short.class.equals(expectedType)) {
      value = rs.getShort(index);
      wasNullCheck = true;
    } else if (Integer.class.equals(expectedType)) {
      value = rs.getInt(index);
      wasNullCheck = true;
    } else if (Long.class.equals(expectedType)) {
      value = rs.getLong(index);
      wasNullCheck = true;
    } else if (Float.class.equals(expectedType)) {
      value = rs.getFloat(index);
      wasNullCheck = true;
    } else if (Double.class.equals(expectedType)
        || Number.class.equals(expectedType)) {
      value = rs.getDouble(index);
      wasNullCheck = true;
    } else if (byte[].class.equals(expectedType)) {
      value = rs.getBytes(index);
    } else if (java.sql.Timestamp.class.equals(expectedType)
        || java.util.Date.class.equals(expectedType)) {
      value = rs.getTimestamp(index);
    } else if (BigDecimal.class.equals(expectedType)) {
      value = rs.getBigDecimal(index);
    } else if (InputStream.class.equals(expectedType)) {
      value = rs.getBinaryStream(index);
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported expected type: %s", expectedType.getName()));
    }

    // Perform was-null check if demanded (for results that the JDBC driver returns as primitives).
    if (wasNullCheck && value != null && rs.wasNull()) {
      value = null;
    }
    return value;
  }

  /**
   * Bind parameter onto PreparedStatement.
   *
   * @param stmt  the prepared statement to bind the parameter onto.
   * @param index the index of the parameter in the statement.
   * @param value the value of the parameter be bound onto the statement.
   * @throws SQLException
   */
  public static void bindParameter(PreparedStatement stmt, int index, Object value)
      throws SQLException {
    if (value == Nulls.NULL_STRING || value instanceof String) {
      // string or clob
      if (value == Nulls.NULL_STRING) {
        stmt.setNull(index, Types.VARCHAR);
      } else {
        stmt.setString(index, (String) value);
      }
    } else if (value == Nulls.NULL_LONG || value instanceof Long) {
      stmt.setLong(index, (Long) value);
    } else if (value == Nulls.NULL_TIMESTAMP || value instanceof java.sql.Timestamp) {
      // we don't support java.sql.Date, and we should not be using that type either.
      stmt.setTimestamp(index, (java.sql.Timestamp) value);
    } else if (value == Nulls.NULL_INTEGER || value instanceof Integer) {
      if (value == Nulls.NULL_INTEGER) {
        stmt.setNull(index, Types.INTEGER);
      } else {
        stmt.setInt(index, (Integer) value);
      }
    } else if (value == Nulls.NULL_BOOLEAN || value instanceof Boolean) {
      stmt.setBoolean(index, (Boolean) value);
    } else if (value == Nulls.NULL_BYTE || value instanceof Byte) {
      stmt.setByte(index, (Byte) value);
    } else if (value == Nulls.NULL_SHORT || value instanceof Short) {
      stmt.setShort(index, (Short) value);
    } else if (value == Nulls.NULL_FLOAT || value instanceof Float) {
      stmt.setFloat(index, (Float) value);
    } else if (value == Nulls.NULL_DOUBLE || value instanceof Double) {
      stmt.setDouble(index, (Double) value);
    } else if (value == Nulls.NULL_BYTE_ARRAY || value instanceof byte[]) {
      // blob
      stmt.setBytes(index, (byte[]) value);
    } else if (value == Nulls.NULL_BIGDECIMAL || value instanceof BigDecimal) {
      stmt.setBigDecimal(index, (BigDecimal) value);
    } else if (value == null) {
      stmt.setNull(index, Types.NULL);
    } else {
      throw new InvalidParameterException(
          String.format("value type not supported: %s", value.getClass().getName()));
    }
  }

  /**
   * Retrieve all the rows satisfying the given SQL query.
   *
   * @param connection   the db connection.
   * @param rawSqlQuery  the raw sql query.
   * @param rowProcessor the processor to assemble the row in resultset to expected object.
   * @param parameters   the parameter to be bound onto the sql query.
   * @param <T>          the expected object type in the result returned.
   * @return a list of objects satisfying the given sql query and filters.
   * @throws SQLException
   */
  public static <T> List<T> select(
      Connection connection,
      String rawSqlQuery,
      RowProcessor<T> rowProcessor,
      Object... parameters) throws IOException, SQLException {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.prepareStatement(rawSqlQuery);

      for (int index = 0; index < parameters.length; index++) {
        // we have to do this because jdbc parameter index starting from 1.
        JdbcUtils.bindParameter(stmt, index + 1, parameters[index]);
      }
      rs = stmt.executeQuery();
      List<T> result = Lists.newLinkedList();
      while (rs.next()) {
        result.add(rowProcessor.process(rs));
      }
      return result;
    } finally {
      JdbcUtils.closeResultSet(rs);
      JdbcUtils.closeStatement(stmt);
    }
  }

  /**
   * A convenience method when only one row is expected from the returned result set. When there
   * are no rows satisfying the criteria, null is returned; when there are more than one row
   * satisfying the criteria, DataservicesRuntimeException is thrown.
   *
   * @throws SQLException when there are more than one row returned from db.
   */
  public static <T> T selectOne(
      Connection connection,
      String rawSqlQuery,
      RowProcessor<T> rowProcessor,
      Object... parameters) throws IOException, SQLException {
    List<T> resultList = JdbcUtils.select(connection, rawSqlQuery, rowProcessor, parameters);
    if (resultList.isEmpty()) {
      return null;
    } else if (resultList.size() == 1) {
      return resultList.get(0);
    } else {
      throw new SQLException(
          String.format("Too many rows returned: %d", resultList.size()));
    }
  }

  /**
   * Insert one row into database with the auto incremental id returned.
   *
   * @param connection  the db connection.
   * @param rawSqlQuery the raw sql query.
   * @param parameters  the parameters to be bound onto the sql query.
   * @return the auto incremental id generated by the db.
   * @throws SQLException
   */
  public static int createWithIdGeneration(
      Connection connection,
      String rawSqlQuery,
      Object... parameters) throws SQLException {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.prepareStatement(rawSqlQuery, Statement.RETURN_GENERATED_KEYS);
      for (int index = 0; index < parameters.length; index++) {
        JdbcUtils.bindParameter(stmt, index + 1, parameters[index]);
      }
      stmt.executeUpdate();
      rs = stmt.getGeneratedKeys();
      rs.next();
      return rs.getInt(1);
    } finally {
      JdbcUtils.closeResultSet(rs);
      JdbcUtils.closeStatement(stmt);
    }
  }

  /**
   * Execute a batch update statement.
   *
   * @param connection         the db connection.
   * @param rawSqlQuery        the raw sql query.
   * @param parametersPerBatch number of parameters in each batch.
   * @param parameters         parameters to be bound onto the sql query.
   * @return number of rows affected per batch.
   * @throws SQLException
   */
  public static int[] executeBatch(
      Connection connection,
      String rawSqlQuery,
      int parametersPerBatch,
      List<Object> parameters) throws SQLException {
    Preconditions.checkArgument(parameters.size() % parametersPerBatch == 0);
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(rawSqlQuery);
      int numBatches = parameters.size() / parametersPerBatch;
      for (int batch = 0; batch < numBatches; batch++) {
        for (int param = 0; param < parametersPerBatch; param++) {
          JdbcUtils.bindParameter(
              stmt, param + 1, parameters.get(batch * parametersPerBatch + param));
        }
        stmt.addBatch();
      }
      return stmt.executeBatch();
    } finally {
      JdbcUtils.closeStatement(stmt);
    }
  }

  /**
   * Execute an insert/update/delete statement with number of affected rows returned.
   *
   * @param connection  the db connection.
   * @param rawSqlQuery the raw sql query.
   * @param parameters  the parameters to be bound onto the sql query.
   * @return the number of rows affected.
   * @throws SQLException
   */
  public static int executeUpdate(
      Connection connection,
      String rawSqlQuery,
      Object... parameters) throws SQLException {
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(rawSqlQuery);
      for (int index = 0; index < parameters.length; index++) {
        JdbcUtils.bindParameter(stmt, index + 1, parameters[index]);
      }
      return stmt.executeUpdate();
    } finally {
      JdbcUtils.closeStatement(stmt);
    }
  }

  /**
   * Execute an statement without any returned result.
   *
   * @param connection  the db connection.
   * @param rawSqlQuery the raw sql query.
   * @param parameters  the parameters to be bound onto the sql query.
   * @throws SQLException
   */
  public static void execute(
      Connection connection,
      String rawSqlQuery,
      Object... parameters) throws SQLException {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.prepareStatement(rawSqlQuery);

      for (int index = 0; index < parameters.length; index++) {
        JdbcUtils.bindParameter(stmt, index + 1, parameters[index]);
      }
      stmt.execute();
    } finally {
      JdbcUtils.closeResultSet(rs);
      JdbcUtils.closeStatement(stmt);
    }
  }
}

