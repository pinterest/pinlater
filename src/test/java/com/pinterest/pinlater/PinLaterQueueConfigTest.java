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
package com.pinterest.pinlater;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)

public class PinLaterQueueConfigTest {

  private static final String TEST_QUEUE_CONFIG =
      "{\n" +
          "    \"queues\": [\n" +
          "        {\n" +
          "            \"name\": \"test_queue_1\",\n" +
          "            \"queueConfig\": {\n" +
          "                \"maxJobsPerSecond\": 0\n" +
          "            }\n" +
          "        },\n" +
          "        {\n" +
          "            \"name\": \"test_queue_2\",\n" +
          "            \"queueConfig\": {\n" +
          "                \"maxJobsPerSecond\": 100\n" +
          "            }\n" +
          "        }\n" +
          "    ]\n" +
          "}";

  private final PinLaterQueueConfig queueConfig = new PinLaterQueueConfig("", "", false);

  @Test
  public void testRateLimit() throws IOException {
    queueConfig.getQueueConfigSchemaRef().set(
        PinLaterQueueConfig.QueueConfigSchema.load(TEST_QUEUE_CONFIG.getBytes()));

    queueConfig.getNumPinLaterServers().set(10);
    queueConfig.rebuild();

    Assert.assertEquals(Double.MAX_VALUE, queueConfig.getDequeueRate("queue_not_in_config"), 0.01);
    Assert.assertTrue(queueConfig.allowDequeue("queue_not_in_config", 1));
    Assert.assertTrue(queueConfig.allowDequeue("queue_not_in_config", 10));
    Assert.assertTrue(queueConfig.allowDequeue("queue_not_in_config", 100));
    Assert.assertTrue(queueConfig.allowDequeue("queue_not_in_config", Integer.MAX_VALUE));

    Assert.assertEquals(0.0, queueConfig.getDequeueRate("test_queue_1"), 0.01);
    Assert.assertFalse(queueConfig.allowDequeue("test_queue_1", 1));
    Assert.assertFalse(queueConfig.allowDequeue("test_queue_1", 10));
    Assert.assertFalse(queueConfig.allowDequeue("test_queue_1", 100));
    Assert.assertFalse(queueConfig.allowDequeue("test_queue_1", Integer.MAX_VALUE));

    Assert.assertEquals(10.0, queueConfig.getDequeueRate("test_queue_2"), 0.01);
    Assert.assertTrue(queueConfig.allowDequeue("test_queue_2", 10));
    Assert.assertFalse(queueConfig.allowDequeue("test_queue_2", 1));

    queueConfig.getNumPinLaterServers().set(1);
    queueConfig.rebuild();

    Assert.assertEquals(Double.MAX_VALUE, queueConfig.getDequeueRate("queue_not_in_config"), 0.01);
    Assert.assertEquals(0.0, queueConfig.getDequeueRate("test_queue_1"), 0.01);
    Assert.assertEquals(100.0, queueConfig.getDequeueRate("test_queue_2"), 0.01);
  }
}
