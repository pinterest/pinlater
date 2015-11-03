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

import com.google.common.base.Preconditions;
import org.apache.commons.lang.NotImplementedException;

import java.util.Random;

/**
 * Dummy Random class which only provides configurable nextInt() and nextInt(int i) API.
 */
public class DummyRandom extends Random {

  private int nextInt = 0;

  public void setNextInt(int i) {
    Preconditions.checkArgument(i >= 0);
    nextInt = i;
  }

  public int nextInt() {
    return nextInt;
  }

  public void nextBytes(byte[] bytes) {
    throw new NotImplementedException();
  }

  public int nextInt(int i) {
    return nextInt;
  }

  public long nextLong() {
    throw new NotImplementedException();
  }

  public boolean nextBoolean() {
    throw new NotImplementedException();
  }

  public float nextFloat() {
    throw new NotImplementedException();
  }

  public double nextDouble() {
    throw new NotImplementedException();
  }

  public synchronized double nextGaussian() {
    throw new NotImplementedException();
  }
}
