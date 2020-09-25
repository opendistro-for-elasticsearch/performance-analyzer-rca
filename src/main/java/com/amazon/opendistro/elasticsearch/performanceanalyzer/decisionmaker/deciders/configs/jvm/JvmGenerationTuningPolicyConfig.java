/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Config;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NestedConfig;

/**
 * "decider-config-settings": {
 *    "jvm-generation-tuning-policy-config": {
 *       "sliding-window-size-in-seconds": 86400
 *       "bucket-size-in-seconds": 3600,
 *       "undersized-bucket-height": 3,
 *       "oversized-bucket-height": 3,
 *    }
 * }
 */
public class JvmGenerationTuningPolicyConfig {
  private static final String SLIDING_WINDOW_SIZE_IN_SECONDS = "bucket-size-in-seconds";
  private static final String BUCKET_SIZE_IN_SECONDS = "bucket-size-in-seconds";
  private static final String UNDERSIZED_BUCKET_HEIGHT = "undersized-bucket-height";
  private static final String OVERSIZED_BUCKET_HEIGHT = "oversized-bucket-height";

  public static final int DEFAULT_SLIDING_WINDOW_SIZE_IN_S = 86400;
  public static final int DEFAULT_BUCKET_SIZE_IN_S = 3600;
  public static final int DEFAULT_UNDERSIZED_BUCKET_HEIGHT = 3;
  public static final int DEFAULT_OVERSIZED_BUCKET_HEIGHT = 3;

  private Config<Integer> bucketSizeInSeconds;
  private Config<Integer> slidingWindowSizeInSeconds;
  private Config<Integer> undersizedbucketHeight;
  private Config<Integer> oversizedbucketHeight;

  public JvmGenerationTuningPolicyConfig(NestedConfig config) {
    slidingWindowSizeInSeconds = new Config<>(SLIDING_WINDOW_SIZE_IN_SECONDS, config.getValue(),
        DEFAULT_SLIDING_WINDOW_SIZE_IN_S, (s) -> (s > 0), Integer.class);
    bucketSizeInSeconds = new Config<>(BUCKET_SIZE_IN_SECONDS, config.getValue(),
        DEFAULT_BUCKET_SIZE_IN_S, (s) -> (s > 0), Integer.class);
    undersizedbucketHeight = new Config<>(UNDERSIZED_BUCKET_HEIGHT, config.getValue(),
        DEFAULT_UNDERSIZED_BUCKET_HEIGHT, (s) -> (s > 0), Integer.class);
    oversizedbucketHeight = new Config<>(OVERSIZED_BUCKET_HEIGHT, config.getValue(),
        DEFAULT_OVERSIZED_BUCKET_HEIGHT, (s) -> (s > 0), Integer.class);
  }

  public int getSlidingWindowSizeInSeconds() {
    return bucketSizeInSeconds.getValue();
  }

  public int getBucketSizeInSeconds() {
    return bucketSizeInSeconds.getValue();
  }

  public int getUndersizedbucketHeight() {
    return undersizedbucketHeight.getValue();
  }

  public int getOversizedbucketHeight() {
    return oversizedbucketHeight.getValue();
  }
}
