/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class SlidingWindowTest {

  @Test
  public void testSlidingWindow() {
    SlidingWindow slidingWindow = new SlidingWindow<>(3, TimeUnit.SECONDS);

    // if the sliding window is empty, return NaN
    Assert.assertTrue(Double.isNaN(slidingWindow.readAvg(TimeUnit.SECONDS)));

    // only one element in window, return NaN
    slidingWindow.next(new SlidingWindowData(0, 1));
    Assert.assertTrue(Double.isNaN(slidingWindow.readAvg(TimeUnit.SECONDS)));

    // 2nd - 4th element
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(1), 10));
    Assert.assertEquals(11, slidingWindow.readAvg(TimeUnit.SECONDS), 0.1);

    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(2), 3));
    Assert.assertEquals(7, slidingWindow.readAvg(TimeUnit.SECONDS), 0.1);

    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(3), 5));
    Assert.assertEquals(6.333, slidingWindow.readAvg(TimeUnit.SECONDS), 0.1);

    //5th element, the sliding window starts to pop out old ones
    slidingWindow.next(new SlidingWindowData(TimeUnit.SECONDS.toMillis(4), 7));
    Assert.assertEquals(8.333, slidingWindow.readAvg(TimeUnit.SECONDS), 0.1);
  }
}
