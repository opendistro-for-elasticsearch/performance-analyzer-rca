/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators;

import java.util.concurrent.TimeUnit;

/**
 * The BucketizedSlidingWindow provides a SlidingWindow implementation that can aggregate all
 * inserts within a configured time range, BUCKET_WINDOW_SIZE, into a single data point.
 *
 * <p>All data within a single bucket window time range is summed by default.
 */
public class BucketizedSlidingWindow extends SlidingWindow<SlidingWindowData> {

  private final long BUCKET_WINDOW_SIZE;

  public BucketizedSlidingWindow(int SLIDING_WINDOW_SIZE, int BUCKET_WINDOW_SIZE, TimeUnit timeUnit) {
    super(SLIDING_WINDOW_SIZE, timeUnit);
    assert BUCKET_WINDOW_SIZE < SLIDING_WINDOW_SIZE : "BucketWindow size should be less than SlidingWindow size";
    this.BUCKET_WINDOW_SIZE = timeUnit.toSeconds(BUCKET_WINDOW_SIZE);
  }

  @Override
  public void next(SlidingWindowData e) {
    if (!windowDeque.isEmpty()) {
      SlidingWindowData firstElement = windowDeque.getFirst();
      if (TimeUnit.MILLISECONDS.toSeconds(e.getTimeStamp() - firstElement.getTimeStamp()) < BUCKET_WINDOW_SIZE) {
        firstElement.value += e.getValue();
        add(e);
        pruneExpiredEntries(e.getTimeStamp());
        return;
      }
    }
    super.next(e);
  }

  public int size() {
    pruneExpiredEntries(System.currentTimeMillis());
    return windowDeque.size();
  }

  @Override
  public double readAvg() {
    pruneExpiredEntries(System.currentTimeMillis());
    return super.readAvg();
  }

  @Override
  public double readAvg(TimeUnit timeUnit) {
    pruneExpiredEntries(System.currentTimeMillis());
    return super.readAvg(timeUnit);
  }

  @Override
  public double readSum() {
    pruneExpiredEntries(System.currentTimeMillis());
    return super.readSum();
  }
}
