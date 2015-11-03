/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.pinterest.pinlater.example;

import com.pinterest.pinlater.thrift.PinLaterJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * An example PinLater job
 */
public class PinLaterExampleJob {

  private static final Logger LOG = LoggerFactory.getLogger(PinLaterExampleJob.class);
  public static final String QUEUE_NAME = "test_queue";

  private String logData;

  public PinLaterExampleJob(String logData) {
    this.logData = logData;
  }

  /**
   * Build a PinLaterJob object that can be used to build an enqueue request.
   */
  public PinLaterJob buildJob() {
    PinLaterJob job = new PinLaterJob(ByteBuffer.wrap(logData.getBytes()));
    job.setNumAttemptsAllowed(10);
    return job;
  }

  public static void process(String logData) throws Exception{
    LOG.info("PinLaterExampleJob: {}", logData);
  }
}
