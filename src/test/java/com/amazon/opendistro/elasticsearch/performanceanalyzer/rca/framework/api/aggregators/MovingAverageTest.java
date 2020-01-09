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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class MovingAverageTest {

  @Test
  public void next() {
    MovingAverage avg = new MovingAverage(3);

    // 1 element is below the window size limit, so the average returns -1.
    Assert.assertEquals(-1, avg.next(1), 0.1);

    // 2 elements are below the window size limit, so the average returns -1.
    Assert.assertEquals(-1, avg.next(10), 0.1);

    // 3 elements is equal to the window size limit, so the average returns the expected value.
    Assert.assertEquals((1 + 10 + 3) / 3.0, avg.next(3), 0.1);
    Assert.assertEquals((10 + 3 + 5) / 3.0, avg.next(5), 0.1);
    Assert.assertEquals((3 + 5 + 7) / 3.0, avg.next(7), 0.1);
  }
}
