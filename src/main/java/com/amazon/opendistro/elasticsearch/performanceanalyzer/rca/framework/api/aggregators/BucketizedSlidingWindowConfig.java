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

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public class BucketizedSlidingWindowConfig {
  private int slidingWindowSizeMinutes;
  private int bucketSizeMinutes;

  private TimeUnit timeUnit;
  private Path persistencePath;

  public BucketizedSlidingWindowConfig(int slidingWindowSizeMinutes,
                                       int bucketSizeMinutes,
                                       TimeUnit timeUnit,
                                       @Nullable Path persistencePath) {
    this.slidingWindowSizeMinutes = slidingWindowSizeMinutes;
    this.bucketSizeMinutes = bucketSizeMinutes;
    this.timeUnit = timeUnit;
    this.persistencePath = persistencePath;
  }

  public int getSlidingWindowSizeMinutes() {
    return slidingWindowSizeMinutes;
  }

  public void setSlidingWindowSizeMinutes(int slidingWindowSizeMinutes) {
    this.slidingWindowSizeMinutes = slidingWindowSizeMinutes;
  }

  public int getBucketSizeMinutes() {
    return bucketSizeMinutes;
  }

  public void setBucketSizeMinutes(int bucketSizeMinutes) {
    this.bucketSizeMinutes = bucketSizeMinutes;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public Path getPersistencePath() {
    return persistencePath;
  }

  public void setPersistencePath(Path persistencePath) {
    this.persistencePath = persistencePath;
  }
}
