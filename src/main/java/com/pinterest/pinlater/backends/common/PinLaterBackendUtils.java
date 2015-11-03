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

import com.pinterest.pinlater.thrift.PinLaterJobInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.twitter.common.base.MorePreconditions;
import com.twitter.util.Function;
import com.twitter.util.Future;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Encapsulates static utility methods used by the different PinLater backend classes.
 */
public final class PinLaterBackendUtils {

  private static final String SALT = "VHzMjY2Kb9BYlyrbtpyFvnHVPZAvB5Rm";

  /**
   * Get the salted hash of a password.
   *
   * @param password  String to be salted and hashed.
   * @return Salted hash of the password string.
   */
  public static String getSaltedHash(String password) throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    String strToDigest = password + SALT;
    byte[] digestedStr = digest.digest(strToDigest.getBytes());
    return new String(Hex.encodeHex(digestedStr));
  }

  /**
   * Executes a batch of requests asynchronously in a partitioned manner,
   * with the specified parallelism.
   *
   * @param requests      List of requests to execute.
   * @param parallelism   Desired parallelism (must be > 0).
   * @param executeBatch  Function to execute each partitioned batch of requests.
   * @param <Req>         Request type.
   * @param <Resp>        Response type.
   * @return List of response futures.
   */
  public static <Req, Resp> List<Future<Resp>> executePartitioned(
      List<Req> requests,
      int parallelism,
      Function<List<Req>, Future<Resp>> executeBatch) {
    MorePreconditions.checkNotBlank(requests);
    Preconditions.checkArgument(parallelism > 0);
    Preconditions.checkNotNull(executeBatch);

    int sizePerPartition = Math.max(requests.size() / parallelism, 1);
    List<List<Req>> partitions = Lists.partition(requests, sizePerPartition);
    List<Future<Resp>> futures = Lists.newArrayListWithCapacity(partitions.size());
    for (final List<Req> request : partitions) {
      futures.add(executeBatch.apply(request));
    }
    return futures;
  }

  /**
   * Merges multiple iterables into a list using the comparing logic provided by the comparator.
   *
   * @param iterablesToMerge  The iterables to be merged.
   * @param comparator        Comparator specifying the comparison logic between the iterables.
   * @param <T>               Iterable item type.
   * @param <S>               Comparator between iterate items type.
   * @return List of all merged results.
   */
  public static <T, S extends Comparator<T>> List<T> mergeIntoList(
      Iterable<? extends Iterable<T>> iterablesToMerge,
      S comparator) {
    return mergeIntoList(iterablesToMerge, comparator, Integer.MAX_VALUE);
  }

  /**
   * Merges multiple iterables into a list using the comparing logic provided by the comparator.
   * The returned list will only include the first n merged items, where n is the limit specified.
   *
   * @param iterablesToMerge  The iterables to be merged.
   * @param comparator        Comparator specifying the comparison logic between the iterables.
   * @param limit             Max number of results that will be returned.
   * @param <T>               Iterable item type.
   * @param <S>               Comparator between iterate items type.
   * @return List of the first n merged results.
   */
  public static <T, S extends Comparator<T>> List<T> mergeIntoList(
      Iterable<? extends Iterable<T>> iterablesToMerge,
      S comparator,
      int limit) {
    // Perform a k-way merge on the collections and return the result in an ArrayList.
    List<T> mergedCols = Lists.newLinkedList();
    Iterator<T> mergeIterator = Iterables.mergeSorted(iterablesToMerge, comparator).iterator();
    while (mergeIterator.hasNext() && mergedCols.size() < limit) {
      mergedCols.add(mergeIterator.next());
    }
    return mergedCols;
  }

  /**
   * Comparator for PinLaterJobInfo: descending in runAfter.
   */
  public static class JobInfoComparator implements Comparator<PinLaterJobInfo> {

    private static JobInfoComparator INSTANCE = new JobInfoComparator();

    private JobInfoComparator() {}

    public static JobInfoComparator getInstance() {
      return INSTANCE;
    }

    public int compare(PinLaterJobInfo a, PinLaterJobInfo b) {
      Long longA = new Long(a.getRunAfterTimestampMillis());
      Long longB = new Long(b.getRunAfterTimestampMillis());
      return -1 * longA.compareTo(longB);
    }
  }
}
