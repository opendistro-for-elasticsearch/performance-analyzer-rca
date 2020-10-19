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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class BucketizedSlidingWindowTest {

  @Test
  public void testBucketization() {
    BucketizedSlidingWindow slidingWindow = new BucketizedSlidingWindow(10, 2, TimeUnit.SECONDS);
    long currTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs), 1));
    assertEquals(1, slidingWindow.size());
    assertEquals(1, slidingWindow.readSum(), 0.00000001);

    // Add data after bucket window size
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs + 3), 2));
    assertEquals(2, slidingWindow.size());
    assertEquals(1 + 2, slidingWindow.readSum(), 0.00000001);
    currTimeInSecs += 3;

    // Add data within bucket window size
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs + 1), 3));
    assertEquals(2, slidingWindow.size());
    assertEquals(1 + (2 + 3), slidingWindow.readSum(), 0.00000001);
    currTimeInSecs += 2;

    // Add data outside sliding window size
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs + 6), 4));
    assertEquals(2, slidingWindow.size());
    assertEquals((2 + 3) + 4, slidingWindow.readSum(), 0.00000001);
    currTimeInSecs += 6;

    // Add data at sliding window size boundary
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs + 10), 5));
    assertEquals(2, slidingWindow.size());
    assertEquals(4 + 5, slidingWindow.readSum(), 0.00000001);
    currTimeInSecs += 10;

    // Add data outside sliding window
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs + 11), 6));
    assertEquals(1, slidingWindow.size());
    assertEquals(6, slidingWindow.readSum(), 0.00000001);
  }

  /**
   * When events are added frequently, with time between consecutive events being
   * less than the bucket window size, we should still see aggregated buckets across
   * the full sliding window timeline.
   */
  @Test
  public void testFrequentEvents() {
    BucketizedSlidingWindow slidingWindow = new BucketizedSlidingWindow(10, 2, TimeUnit.SECONDS);
    long currTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    for (long t = currTimeInSecs; t < currTimeInSecs + 4; t++) {
      slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(t), 1));
    }
    assertEquals(2, slidingWindow.size());
  }

  @Test
  public void testReadAvgWithPartialPruning() {
    BucketizedSlidingWindow slidingWindow = new BucketizedSlidingWindow(10, 1, TimeUnit.SECONDS);
    long currTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 11), 11));
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 9), 9));
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 7), 7));
    assertEquals(3, slidingWindow.windowDeque.size());  // we cannot call slidingWindow.size() as it also prunes expired elements
    assertEquals((double)(9 + 7) / (double)2, slidingWindow.readAvg(), 0.00000001);
    assertEquals(2, slidingWindow.size());
  }

  @Test
  public void testReadAvgWithFullPruning() {
    BucketizedSlidingWindow slidingWindow = new BucketizedSlidingWindow(10, 1, TimeUnit.SECONDS);
    long currTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 11), 11));
    assertTrue(Double.isNaN(slidingWindow.readAvg()));
    assertEquals(0, slidingWindow.size());
  }

  @Test
  public void testReadTimeAvgWithPartialPruning() {
    BucketizedSlidingWindow slidingWindow = new BucketizedSlidingWindow(10, 1, TimeUnit.SECONDS);
    long currTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 11), 11));
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 9), 9));
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 7), 5));
    assertEquals(3, slidingWindow.windowDeque.size());  // we cannot call slidingWindow.size() as it also prunes expired elements
    assertEquals((double)(9 + 5) / (double)2, slidingWindow.readAvg(TimeUnit.SECONDS), 0.00000001);
    assertEquals(2, slidingWindow.size());
  }

  @Test
  public void testReadTimeAvgWithFullPruning() {
    BucketizedSlidingWindow slidingWindow = new BucketizedSlidingWindow(10, 1, TimeUnit.SECONDS);
    long currTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 11), 11));
    assertTrue(Double.isNaN(slidingWindow.readAvg(TimeUnit.SECONDS)));
    assertEquals(0, slidingWindow.size());
  }

  @Test
  public void testReadSumWithPartialPruning() {
    BucketizedSlidingWindow slidingWindow = new BucketizedSlidingWindow(10, 1, TimeUnit.SECONDS);
    long currTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 11), 11));
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 9), 9));
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 7), 5));
    assertEquals(3, slidingWindow.windowDeque.size());  // we cannot call slidingWindow.size() as it also prunes expired elements
    assertEquals((double)(9 + 5), slidingWindow.readSum(), 0.00000001);
    assertEquals(2, slidingWindow.size());
  }

  @Test
  public void testReadSumWithFullPruning() {
    BucketizedSlidingWindow slidingWindow = new BucketizedSlidingWindow(10, 1, TimeUnit.SECONDS);
    long currTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 11), 11));
    assertEquals(0, slidingWindow.readSum(), 0.000001);
    assertEquals(0, slidingWindow.size());
  }

  @Test
  public void testSizeWithPartialPruning() {
    BucketizedSlidingWindow slidingWindow = new BucketizedSlidingWindow(10, 1, TimeUnit.SECONDS);
    long currTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 11), 11));
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 9), 9));
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 7), 5));
    assertEquals(2, slidingWindow.size());
  }

  @Test
  public void testSizeWithFullPruning() {
    BucketizedSlidingWindow slidingWindow = new BucketizedSlidingWindow(10, 1, TimeUnit.SECONDS);
    long currTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(currTimeInSecs - 11), 11));
    assertEquals(0, slidingWindow.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInit() {
    BucketizedSlidingWindow slidingWindow = new BucketizedSlidingWindow(10, 10, TimeUnit.SECONDS);
  }
}
