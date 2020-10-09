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

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The BucketizedSlidingWindow provides a SlidingWindow implementation that can aggregate all
 * inserts within a configured time range, BUCKET_WINDOW_SIZE, into a single data point.
 *
 * <p>All data within a single bucket window time range is summed by default.
 */
public class BucketizedSlidingWindow extends PersistableSlidingWindow {
  private static final Logger LOG = LogManager.getLogger(BucketizedSlidingWindow.class);
  private final long BUCKET_WINDOW_SIZE;

  /**
   * Creates a BucketizedSlidingWindow which won't persist data on disk
   * @param SLIDING_WINDOW_SIZE Length of the window in units of time
   * @param BUCKET_WINDOW_SIZE Length of each bucket in units of time
   * @param timeUnit The unit of time
   */
  public BucketizedSlidingWindow(int SLIDING_WINDOW_SIZE, int BUCKET_WINDOW_SIZE, TimeUnit timeUnit) {
    this(SLIDING_WINDOW_SIZE, BUCKET_WINDOW_SIZE, timeUnit, null);
  }

  /**
   * Creates a BucketizedSlidingWindow which will persist data on disk
   * @param SLIDING_WINDOW_SIZE Length of the window in units of time
   * @param BUCKET_WINDOW_SIZE Length of each bucket in units of time
   * @param timeUnit The unit of time
   * @param persistFilePath Path to the file to use for persistence
   */
  public BucketizedSlidingWindow(int SLIDING_WINDOW_SIZE, int BUCKET_WINDOW_SIZE, TimeUnit timeUnit, Path persistFilePath) {
    super(SLIDING_WINDOW_SIZE, timeUnit, persistFilePath);
    assert BUCKET_WINDOW_SIZE < SLIDING_WINDOW_SIZE : "BucketWindow size should be less than SlidingWindow size";
    this.BUCKET_WINDOW_SIZE = timeUnit.toMillis(BUCKET_WINDOW_SIZE);
  }

  @Override
  public void next(SlidingWindowData e) {
    if (!windowDeque.isEmpty()) {
      SlidingWindowData firstElement = windowDeque.getFirst();
      if ((e.getTimeStamp() - firstElement.getTimeStamp()) < BUCKET_WINDOW_SIZE) {
        firstElement.value += e.getValue();
        add(e);
        pruneExpiredEntries(e.getTimeStamp());
        try {
          write(); // Try to persist the data whenever we complete writing a bucket
        } catch (IOException ex) {
          LOG.error("Failed to persist {} data", this.getClass().getSimpleName(), ex);
        }
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
