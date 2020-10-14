/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 * This is a generic sliding window for RCA sampling. The default behavior is to store the
 * {timestap, value} pair and maintain the sum of all data entries within this sliding window.
 */
public class SlidingWindow<E extends SlidingWindowData> {

  protected final Deque<E> windowDeque;
  protected final long SLIDING_WINDOW_SIZE;
  protected double sum;

  public SlidingWindow(int SLIDING_WINDOW_SIZE_IN_TIMESTAMP, TimeUnit timeUnit) {
    this.windowDeque = new LinkedList<>();
    this.SLIDING_WINDOW_SIZE = timeUnit.toSeconds(SLIDING_WINDOW_SIZE_IN_TIMESTAMP);
    this.sum = 0.0;
  }

  /**
   * callback function when adding a data to the sliding window
   */
  protected void add(E e) {
    sum += e.getValue();
  }

  /**
   * callback function when removing a data from the sliding window
   */
  protected void remove(E e) {
    sum -= e.getValue();
  }

  protected void pruneExpiredEntries(long endTimeStamp) {
    while (!windowDeque.isEmpty()
        && TimeUnit.MILLISECONDS.toSeconds(endTimeStamp - windowDeque.peekLast().getTimeStamp()) > SLIDING_WINDOW_SIZE) {
      E lastData = windowDeque.pollLast();
      remove(lastData);
    }
  }

  /**
   * insert data into the sliding window
   */
  public void next(E e) {
    pruneExpiredEntries(e.getTimeStamp());
    add(e);
    windowDeque.addFirst(e);
  }

  /**
   * read the sliding window average based on sliding window size
   */
  public double readAvg() {
    if (!windowDeque.isEmpty()) {
      return sum / (double) windowDeque.size();
    }
    return Double.NaN;
  }

  /**
   * read the sliding window average based on timestamp
   */
  public double readAvg(TimeUnit timeUnit) {
    if (windowDeque.isEmpty()) {
      return Double.NaN;
    }
    long timeStampDiff = windowDeque.peekFirst().getTimeStamp() - windowDeque.peekLast().getTimeStamp();
    if (timeStampDiff > 0) {
      return sum / ((double) timeStampDiff / (double) timeUnit.toMillis(1));
    }
    return Double.NaN;
  }

  /**
   * read the sliding window sum
   */
  public double readSum() {
    return this.sum;
  }

  public int size() {
    return windowDeque.size();
  }

}
