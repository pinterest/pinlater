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
package com.pinterest.pinlater.backends.common;

import com.google.common.base.Preconditions;
import com.twitter.common.base.MorePreconditions;

/**
 * Helper class used by the different backends to encapsulate routines dealing
 * with job descriptors.
 */
public class PinLaterJobDescriptor {

  private final String queueName;
  private final String shardName;
  private final int priority;
  private final long localId;
  private final String formattedString;

  public PinLaterJobDescriptor(String queueName, String shardName, int priority, long localId) {
    this.queueName = MorePreconditions.checkNotBlank(queueName);
    this.shardName = MorePreconditions.checkNotBlank(shardName);
    Preconditions.checkArgument(priority >= 0);
    this.priority = priority;
    Preconditions.checkArgument(localId >= 0);
    this.localId = localId;

    // NOTE: Queue names are restricted to alphanumeric + underscores by API contract, enforced
    // above the backend layer. We don't re-enforce in this class for performance reasons.
    this.formattedString = String.format("%s:s%s:p%d:%d", queueName, shardName, priority, localId);
  }

  public PinLaterJobDescriptor(String jobDescriptor) {
    this.formattedString = MorePreconditions.checkNotBlank(jobDescriptor);

    String[] tokens = jobDescriptor.split(":");
    Preconditions.checkArgument(tokens.length == 4);

    this.queueName = tokens[0];
    this.shardName = tokens[1].substring(1);
    this.priority = Integer.parseInt(tokens[2].substring(1));
    this.localId = Long.parseLong(tokens[3]);

  }

  public String toString() {
    return formattedString;
  }

  public String getQueueName() {
    return queueName;
  }

  public String getShardName() {
    return shardName;
  }

  public int getPriority() {
    return priority;
  }

  public long getLocalId() {
    return localId;
  }
}
