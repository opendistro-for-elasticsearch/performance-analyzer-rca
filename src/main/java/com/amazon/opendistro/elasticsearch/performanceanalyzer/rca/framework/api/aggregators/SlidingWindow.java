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
public class SlidingWindow {

  protected final Deque<SlidingWindowData> windowDeque;
  protected final int SLIDING_WINDOW_SIZE_IN_SECONDS;
  protected double sum;

  public SlidingWindow(int SLIDING_WINDOW_SIZE_IN_SECONDS) {
    this.windowDeque = new LinkedList<>();
    this.SLIDING_WINDOW_SIZE_IN_SECONDS = SLIDING_WINDOW_SIZE_IN_SECONDS;
    this.sum = 0.0;
  }

  /**
   * callback function when adding a data to the sliding window
   */
  protected void add(double value) {
    sum += value;
  }

  /**
   * callback function when removing a data from the sliding window
   */
  protected void remove(double value) {
    sum -= value;
  }

  /**
   * insert data into the sliding window
   */
  public void next(long timeStamp, double value) {
    while (!windowDeque.isEmpty()
        && TimeUnit.MILLISECONDS.toSeconds(timeStamp - windowDeque.peekLast().getTimeStamp())
        > SLIDING_WINDOW_SIZE_IN_SECONDS) {
      double lastVal = windowDeque.pollLast().getValue();
      remove(lastVal);
    }
    add(value);
    windowDeque.addFirst(new SlidingWindowData(timeStamp, value));
  }

  /**
   * read the sliding window average
   */
  public double read() {
    if (!windowDeque.isEmpty()) {
      long timeStampDiff =
          windowDeque.peekFirst().getTimeStamp() - windowDeque.peekLast().getTimeStamp();
      if (timeStampDiff > 0) {
        return sum / (double) TimeUnit.MILLISECONDS.toSeconds(timeStampDiff);
      } else {
        return Double.NaN;
      }
    }
    return Double.NaN;
  }
}
