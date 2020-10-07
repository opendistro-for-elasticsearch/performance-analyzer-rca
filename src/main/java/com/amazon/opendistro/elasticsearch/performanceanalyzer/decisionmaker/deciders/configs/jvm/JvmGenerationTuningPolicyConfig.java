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
 * Configures various options for the
 * {@link com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.JvmGenerationTuningPolicy}
 *
 * <p>The config follows the format below
 *  "decider-config-settings": {
 *    "jvm-generation-tuning-policy-config": {
 *       "enabled": true,
 *       "sliding-window-size-in-seconds": 86400,
 *       "bucket-size-in-seconds": 3600,
 *       "undersized-bucket-height": 3,
 *       "oversized-bucket-height": 3,
 *       "should-decrease-young-gen": false,
 *       ...
 *    }
 * }
 */
public class JvmGenerationTuningPolicyConfig {
  private static final String ENABLED = "enabled";
  private static final String SLIDING_WINDOW_SIZE_IN_SECONDS = "sliding-window-size-in-seconds";
  private static final String BUCKET_SIZE_IN_SECONDS = "bucket-size-in-seconds";
  private static final String UNDERSIZED_BUCKET_HEIGHT = "undersized-bucket-height";
  private static final String OVERSIZED_BUCKET_HEIGHT = "oversized-bucket-height";
  private static final String SHOULD_DECREASE_YOUNG_GEN = "should-decrease-young-gen";

  public static final boolean DEFAULT_ENABLED = false;
  public static final int DEFAULT_SLIDING_WINDOW_SIZE_IN_S = 86400;
  public static final int DEFAULT_BUCKET_SIZE_IN_S = 3600;
  public static final int DEFAULT_UNDERSIZED_BUCKET_HEIGHT = 3;
  public static final int DEFAULT_OVERSIZED_BUCKET_HEIGHT = 3;
  public static final boolean DEFAULT_SHOULD_DECREASE_YOUNG_GEN = false;

  private Config<Boolean> enabled;
  private Config<Integer> bucketSizeInSeconds;
  private Config<Integer> slidingWindowSizeInSeconds;
  private Config<Integer> undersizedbucketHeight;
  private Config<Integer> oversizedbucketHeight;
  private Config<Boolean> shouldDecreaseYoungGen;

  public JvmGenerationTuningPolicyConfig(NestedConfig config) {
    enabled = new Config<>(ENABLED, config.getValue(), DEFAULT_ENABLED, Boolean.class);
    slidingWindowSizeInSeconds = new Config<>(SLIDING_WINDOW_SIZE_IN_SECONDS, config.getValue(),
        DEFAULT_SLIDING_WINDOW_SIZE_IN_S, (s) -> (s > 0), Integer.class);
    bucketSizeInSeconds = new Config<>(BUCKET_SIZE_IN_SECONDS, config.getValue(),
        DEFAULT_BUCKET_SIZE_IN_S, (s) -> (s > 0), Integer.class);
    undersizedbucketHeight = new Config<>(UNDERSIZED_BUCKET_HEIGHT, config.getValue(),
        DEFAULT_UNDERSIZED_BUCKET_HEIGHT, (s) -> (s > 0), Integer.class);
    oversizedbucketHeight = new Config<>(OVERSIZED_BUCKET_HEIGHT, config.getValue(),
        DEFAULT_OVERSIZED_BUCKET_HEIGHT, (s) -> (s > 0), Integer.class);
    shouldDecreaseYoungGen = new Config<>(SHOULD_DECREASE_YOUNG_GEN, config.getValue(),
        DEFAULT_SHOULD_DECREASE_YOUNG_GEN, Boolean.class);
  }

  /**
   * Whether or not to enable the policy. A disabled policy will not emit any actions.
   * @return Whether or not to enable the policy
   */
  public boolean isEnabled() {
    return enabled.getValue();
  }

  /**
   * The length of the sliding window used to detect generational sizing issues
   * @return The length of the sliding window used to detect generational sizing issues
   */
  public int getSlidingWindowSizeInSeconds() {
    return bucketSizeInSeconds.getValue();
  }

  /**
   * How many "young gen is too small" issues we should see before we conclude it's unhealthy
   * @return How many "young gen is too small" issues we should see before we conclude it's unhealthy
   */
  public int getUndersizedbucketHeight() {
    return undersizedbucketHeight.getValue();
  }

  /**
   * How many "young gen is too large" issues we should see before we conclude it's unhealthy
   * @return How many "young gen is too large" issues we should see before we conclude it's unhealthy
   */
  public int getOversizedbucketHeight() {
    return oversizedbucketHeight.getValue();
  }

  /**
   * Whether or not the policy should suggest scaling down the young generation
   * @return Whether or not the policy should suggest scaling down the young generation
   */
  public boolean shouldDecreaseYoungGen() {
    return shouldDecreaseYoungGen.getValue();
  }
}
