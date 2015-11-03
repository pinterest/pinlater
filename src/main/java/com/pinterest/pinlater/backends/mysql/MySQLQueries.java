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

import com.google.common.annotations.VisibleForTesting;

/**
 * Encapsulates MySQL queries used by the PinLaterMySQLBackend and related classes.
 */
public final class MySQLQueries {

  private MySQLQueries() {}

  @VisibleForTesting
  public static final int CUSTOM_STATUS_SIZE_BYTES = 5000;

  private static final String TRUNCATED_CUSTOM_STATUS =
      "SUBSTRING(?, 1, " + CUSTOM_STATUS_SIZE_BYTES + ")";

  private static final String TRUNCATED_CONCAT_CUSTOM_STATUS =
      "SUBSTRING(CONCAT(custom_status, ?), 1, " + CUSTOM_STATUS_SIZE_BYTES + ")";

  private static final String TRUNCATED_PREPEND_CUSTOM_STATUS =
      "SUBSTRING(CONCAT(?, custom_status), 1, " + CUSTOM_STATUS_SIZE_BYTES + ")";

  public static final String BODY_REGEX_CLAUSE = "AND body REGEXP '%s'";

  public static final String CREATE_DATABASE =
      "CREATE DATABASE %s";

  public static final String CREATE_JOBS_TABLE =
      "CREATE TABLE %s"
          + "( local_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT"
          + ", state TINYINT NOT NULL"
          + ", claim_descriptor VARCHAR(200)"
          + ", attempts_allowed INT"
          + ", attempts_remaining INT NOT NULL"
          + ", custom_status VARCHAR(" + CUSTOM_STATUS_SIZE_BYTES + ")"
          + ", updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
          + ", created_at TIMESTAMP NOT NULL"
          + ", run_after TIMESTAMP NOT NULL"
          + ", body BLOB NOT NULL"
          + ", PRIMARY KEY (local_id), INDEX (state, run_after), INDEX (claim_descriptor)"
          + " ) ENGINE=INNODB";

  public static final String ENQUEUE_INSERT =
      "INSERT INTO %s"
          + " (state, attempts_allowed, attempts_remaining, custom_status, created_at, run_after,"
          + "  body)"
          + " VALUES (?, ?, ?, " + TRUNCATED_CUSTOM_STATUS + ", ?, ?, ?)";

  public static final String DEQUEUE_UPDATE =
      "UPDATE %s"
          + " SET claim_descriptor = ?, state = ?"
          + " WHERE state = ? AND run_after <= NOW()"
          + " LIMIT ?";

  public static final String DEQUEUE_SELECT =
      "SELECT local_id, attempts_allowed, attempts_remaining, updated_at, created_at, "
          + "       body FROM %s"
          + " WHERE claim_descriptor = ?";

  public static final String DEQUEUE_DRY_RUN_SELECT =
      "SELECT local_id, attempts_allowed, attempts_remaining, updated_at, created_at, "
          + "       body FROM %s"
          + " WHERE state = ? AND run_after <= NOW()"
          + " LIMIT ?";

  public static final String LOOKUP_JOB =
      "SELECT local_id, state, attempts_allowed, attempts_remaining, created_at, run_after, "
          + "       updated_at, claim_descriptor, custom_status FROM %s"
          + " WHERE local_id = ?";

  public static final String LOOKUP_JOB_WITH_BODY =
      "SELECT local_id, state, attempts_allowed, attempts_remaining, created_at, run_after, "
          + "       updated_at, claim_descriptor, custom_status, body FROM %s"
          + " WHERE local_id = ?";

  // Note: a nested query is used here in order to allow for us to cap the count at a certain
  // threshold. The reason for this is executing count queries for millions of rows will take on
  // the order of seconds, which is too slow. For reference, a count query capped with a limit of
  // 100k runs in about ~0.1 s.
  public static final String COUNT_CURRENT_JOBS_BY_STATE_PRIORITY =
      "SELECT COUNT(*) FROM ("
          + " SELECT 1 FROM %s"
          + "  WHERE state = ? AND run_after <= NOW() %s"
          + "  LIMIT ?) as job_info";

  public static final String COUNT_FUTURE_JOBS_BY_STATE_PRIORITY =
      "SELECT COUNT(*) FROM ("
          + " SELECT 1 FROM %s"
          + "  WHERE state = ? AND run_after > NOW() %s"
          + "  LIMIT ?) as job_info";

  public static final String SCAN_CURRENT_JOBS =
      "SELECT local_id, claim_descriptor, attempts_allowed, attempts_remaining, custom_status,"
          + "       created_at, run_after, updated_at FROM %s"
          + " WHERE state = ? AND run_after <= NOW() %s"
          + " ORDER BY run_after DESC"
          + " LIMIT ?";

  public static final String SCAN_FUTURE_JOBS =
      "SELECT local_id, claim_descriptor, attempts_allowed, attempts_remaining, custom_status,"
          + "       created_at, run_after, updated_at FROM %s"
          + " WHERE state = ? AND run_after > NOW() %s"
          + " ORDER BY run_after DESC"
          + " LIMIT ?";

  public static final String RETRY_FAILED_JOBS =
      "UPDATE %s"
          + " SET state = ?, attempts_remaining = ?, run_after = ?"
          + " WHERE state = ? "
          + " LIMIT ?";

  public static final String DELETE_JOBS =
      "DELETE FROM %s"
          + " WHERE state = ? %s"
          + " LIMIT ?";

  public static final String ACK_SUCCEEDED_UPDATE =
      "UPDATE %s"
          + " SET state = ?, "
          + "     custom_status = " + TRUNCATED_CONCAT_CUSTOM_STATUS
          + " WHERE local_id = ?";

  public static final String ACK_FAILED_DONE_UPDATE =
      "UPDATE %s"
          + " SET state = ?, "
          + "     custom_status = " + TRUNCATED_CONCAT_CUSTOM_STATUS + ","
          + "     attempts_remaining = 0"
          + " WHERE local_id = ? AND state = ? AND attempts_remaining = 1";

  public static final String ACK_FAILED_RETRY_UPDATE =
      "UPDATE %s"
          + " SET state = ?, "
          + "     custom_status = " + TRUNCATED_CONCAT_CUSTOM_STATUS + ","
          + "     run_after = ?,"
          + "     claim_descriptor = NULL,"
          + "     attempts_remaining = attempts_remaining - 1"
          + " WHERE local_id = ? AND state = ? AND attempts_remaining > 1";

  public static final String CHECKPOINT_JOB_HEADER =
      "UPDATE %s"
          + " SET state = ?"
          + ",    run_after = ?";

  public static final String CHECKPOINT_JOB_SET_BODY =
      ",     body = ?";

  public static final String CHECKPOINT_JOB_RESET_ATTEMPTS =
      ",     attempts_allowed = ?"
          + ",     attempts_remaining = ?";

  public static final String CHECKPOINT_JOB_APPEND_CUSTOM_STATUS =
      ",     custom_status = " + TRUNCATED_PREPEND_CUSTOM_STATUS;

  public static final String CHECKPOINT_JOB_RESET_CLAIM_DESCRIPTOR =
      ",     claim_descriptor = NULL";

  // We match the claim descriptor when processing a checkpoint request because there can be jobs
  // running on workers for a long time that incur a server-side timeout but actually end up
  // finishing later on and try to checkpoint even though a second worker is already working
  // on the job. For these cases, we would like to make the stale checkpoint request a no-op.
  public static final String CHECKPOINT_JOB_FOOTER =
      " WHERE local_id = ? AND state = ? AND claim_descriptor LIKE ?";

  public static final String MONITOR_TIMEOUT_DONE_UPDATE =
      "UPDATE %s"
          + " SET state = ?, attempts_remaining = 0"
          + " WHERE state = ? AND updated_at < ? AND attempts_remaining = 1 LIMIT ?";

  public static final String MONITOR_TIMEOUT_RETRY_UPDATE =
      "UPDATE %s"
          + " SET state = ?, claim_descriptor = NULL, attempts_remaining = attempts_remaining - 1"
          + " WHERE state = ? AND updated_at < ? AND attempts_remaining > 1 LIMIT ?";

  // Note: we intentionally use run_after instead of the more appropriate updated_at here
  // for query efficiency. In practice, given that the GC time period is long, these should
  // be roughly equivalent.
  public static final String MONITOR_GC_DONE_JOBS =
      "DELETE FROM %s"
          + " WHERE state = ? AND run_after < ? LIMIT ?";

  public static final String DROP_DATABASE =
      "DROP DATABASE IF EXISTS %s";
}
