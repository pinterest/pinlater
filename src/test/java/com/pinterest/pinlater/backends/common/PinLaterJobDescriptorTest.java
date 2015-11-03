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

import org.junit.Assert;
import org.junit.Test;

public class PinLaterJobDescriptorTest {

  @Test
  public void testBasicFunctionality() {
    PinLaterJobDescriptor jobDesc1 = new PinLaterJobDescriptor("job_desc_test", "1", 2, 12345L);
    Assert.assertEquals("job_desc_test", jobDesc1.getQueueName());
    Assert.assertEquals("1", jobDesc1.getShardName());
    Assert.assertEquals(2, jobDesc1.getPriority());
    Assert.assertEquals(12345L, jobDesc1.getLocalId());

    PinLaterJobDescriptor jobDesc2 = new PinLaterJobDescriptor(jobDesc1.toString());
    Assert.assertEquals(jobDesc1.getQueueName(), jobDesc2.getQueueName());
    Assert.assertEquals(jobDesc1.getShardName(), jobDesc2.getShardName());
    Assert.assertEquals(jobDesc1.getPriority(), jobDesc2.getPriority());
    Assert.assertEquals(jobDesc1.getLocalId(), jobDesc2.getLocalId());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueueNameEmptyValidation() {
    new PinLaterJobDescriptor("", "1", 2, 12345L);
  }

  @Test(expected = NullPointerException.class)
  public void testQueueNameNullValidation() {
    new PinLaterJobDescriptor(null, "1", 2, 12345L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testShardNameEmptyValidation() {
    new PinLaterJobDescriptor("job_desc_test", "", 2, 12345L);
  }

  @Test(expected = NullPointerException.class)
  public void testShardNameNullValidation() {
    new PinLaterJobDescriptor("job_desc_test", null, 2, 12345L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPriorityValidation() {
    new PinLaterJobDescriptor("job_desc_test", "1", -2, 12345L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLocalIdValidation() {
    new PinLaterJobDescriptor("job_desc_test", "1", 2, -12345L);
  }
}
