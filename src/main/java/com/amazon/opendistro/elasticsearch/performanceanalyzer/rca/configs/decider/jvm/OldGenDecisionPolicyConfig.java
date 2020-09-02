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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Config;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NestedConfig;

/**
 * "decider-config-settings": {
 *    "old-gen-decision-policy-config": {
 *       "queue-bucket-size": 10,
 *       "queue-step-count": 20,
 *       "cache-step-count": 20,
 *       "old-gen-threshold-level-one": 0.6,
 *       "old-gen-threshold-level-two": 0.75,
 *       "old-gen-threshold-level-three": 0.9,
 *       "level-one-config": {
 *         ......
 *       },
 *       "level-two-config": {
 *        ......
 *       }
 *    }
 * }
 */
public class OldGenDecisionPolicyConfig {
  private static final String QUEUE_BUCKET_SIZE_CONFIG_NAME = "queue-bucket-size";
  private static final String QUEUE_STEP_COUNT_CONFIG_NAME = "queue-step-count";
  private static final String CACHE_STEP_COUNT_CONFIG_NAME = "cache-step-count";
  private static final String LEVEL_ONE_CONFIG_NAME = "level-one-config";
  private static final String LEVEL_TWO_CONFIG_NAME = "level-two-config";
  private static final String LEVEL_THREE_CONFIG_NAME = "level-three-config";
  private static final String OLD_GEN_THRESHOLD_LEVEL_ONE_CONFIG_NAME = "old-gen-threshold-level-one";
  private static final String OLD_GEN_THRESHOLD_LEVEL_TWO_CONFIG_NAME = "old-gen-threshold-level-two";
  private static final String OLD_GEN_THRESHOLD_LEVEL_THREE_CONFIG_NAME = "old-gen-threshold-level-three";
  public static final int DEFAULT_QUEUE_BUCKET_SIZE = 10;
  public static final int DEFAULT_QUEUE_STEP_COUNT = 20;
  public static final int DEFAULT_CACHE_STEP_COUNT = 20;
  public static final double DEFAULT_OLD_GEN_THRESHOLD_LEVEL_ONE = 0.60;
  public static final double DEFAULT_OLD_GEN_THRESHOLD_LEVEL_TWO = 0.75;
  public static final double DEFAULT_OLD_GEN_THRESHOLD_LEVEL_THREE = 0.9;

  private final LevelOneActionBuilderConfig levelOneActionBuilderConfig;
  private final LevelTwoActionBuilderConfig levelTwoActionBuilderConfig;
  private final LevelThreeActionBuilderConfig levelThreeActionBuilderConfig;
  private Config<Double> oldGenThresholdLevelOne;
  private Config<Double> oldGenThresholdLevelTwo;
  private Config<Double> oldGenThresholdLevelThree;
  private Config<Integer> queueBucketSize;
  private Config<Integer> queueStepCount;
  private Config<Integer> cacheStepCount;

  public OldGenDecisionPolicyConfig(NestedConfig oldGenConfig) {
    levelOneActionBuilderConfig = new LevelOneActionBuilderConfig(new NestedConfig(LEVEL_ONE_CONFIG_NAME, oldGenConfig.getValue()));
    levelTwoActionBuilderConfig = new LevelTwoActionBuilderConfig(new NestedConfig(LEVEL_TWO_CONFIG_NAME, oldGenConfig.getValue()));
    levelThreeActionBuilderConfig = new LevelThreeActionBuilderConfig(new NestedConfig(LEVEL_THREE_CONFIG_NAME, oldGenConfig.getValue()));
    oldGenThresholdLevelOne = new Config<>(OLD_GEN_THRESHOLD_LEVEL_ONE_CONFIG_NAME, oldGenConfig.getValue(),
        DEFAULT_OLD_GEN_THRESHOLD_LEVEL_ONE, (s) -> (s > 0), Double.class);
    oldGenThresholdLevelTwo = new Config<>(OLD_GEN_THRESHOLD_LEVEL_TWO_CONFIG_NAME, oldGenConfig.getValue(),
        DEFAULT_OLD_GEN_THRESHOLD_LEVEL_TWO, (s) -> (s > oldGenThresholdLevelOne.getValue()), Double.class);
    oldGenThresholdLevelThree = new Config<>(OLD_GEN_THRESHOLD_LEVEL_THREE_CONFIG_NAME, oldGenConfig.getValue(),
        DEFAULT_OLD_GEN_THRESHOLD_LEVEL_THREE, (s) -> (s > oldGenThresholdLevelTwo.getValue()), Double.class);
    queueBucketSize = new Config<>(QUEUE_BUCKET_SIZE_CONFIG_NAME, oldGenConfig.getValue(),
        DEFAULT_QUEUE_BUCKET_SIZE, (s) -> (s > 0), Integer.class);
    queueStepCount = new Config<>(QUEUE_STEP_COUNT_CONFIG_NAME, oldGenConfig.getValue(),
        DEFAULT_QUEUE_STEP_COUNT, (s) -> (s > 0), Integer.class);
    cacheStepCount = new Config<>(CACHE_STEP_COUNT_CONFIG_NAME, oldGenConfig.getValue(),
        DEFAULT_CACHE_STEP_COUNT, (s) -> (s > 0), Integer.class);
  }

  public LevelOneActionBuilderConfig levelOneActionBuilderConfig() {
    return levelOneActionBuilderConfig;
  }

  public LevelTwoActionBuilderConfig levelTwoActionBuilderConfig() {
    return levelTwoActionBuilderConfig;
  }

  public LevelThreeActionBuilderConfig levelThreeActionBuilderConfig() {
    return levelThreeActionBuilderConfig;
  }

  public double oldGenThresholdLevelOne() {
    return oldGenThresholdLevelOne.getValue();
  }

  public double oldGenThresholdLevelTwo() {
    return oldGenThresholdLevelTwo.getValue();
  }

  public double oldGenThresholdLevelThree() {
    return oldGenThresholdLevelThree.getValue();
  }

  public int queueBucketSize() {
    return queueBucketSize.getValue();
  }

  public int queueStepCount() {
    return queueStepCount.getValue();
  }

  public int cacheStepCount() {
    return cacheStepCount.getValue();
  }
}
