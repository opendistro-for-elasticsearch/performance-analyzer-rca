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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.ConfigUtils;
import java.util.Map;

/**
 * "decider-config-settings": {
 *    "old-gen-decision-policy-config": {
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
  private static final String LEVEL_ONE_CONFIG_NAME = "level-one-config";
  private static final String LEVEL_TWO_CONFIG_NAME = "level-two-config";
  private static final String OLD_GEN_THRESHOLD_LEVEL_ONE_CONFIG_NAME = "old-gen-threshold-level-one";
  private static final String OLD_GEN_THRESHOLD_LEVEL_TWO_CONFIG_NAME = "old-gen-threshold-level-two";
  private static final String OLD_GEN_THRESHOLD_LEVEL_THREE_CONFIG_NAME = "old-gen-threshold-level-three";
  public static final double DEFAULT_OLD_GEN_THRESHOLD_LEVEL_ONE = 0.60;
  public static final double DEFAULT_OLD_GEN_THRESHOLD_LEVEL_TWO = 0.75;
  public static final double DEFAULT_OLD_GEN_THRESHOLD_LEVEL_THREE = 0.9;

  private final LevelOneActionBuilderConfig levelOneActionBuilderConfig;
  private final LevelTwoActionBuilderConfig levelTwoActionBuilderConfig;
  private Double oldGenThresholdLevelOne;
  private Double oldGenThresholdLevelTwo;
  private Double oldGenThresholdLevelThree;

  public OldGenDecisionPolicyConfig(Map<String, Object> configs) {
    Map<String, Object> levelOneConfig = ConfigUtils.readConfig(configs, LEVEL_ONE_CONFIG_NAME, Map.class);
    levelOneActionBuilderConfig = new LevelOneActionBuilderConfig(levelOneConfig);
    Map<String, Object> levelTwoConfig = ConfigUtils.readConfig(configs, LEVEL_TWO_CONFIG_NAME, Map.class);
    levelTwoActionBuilderConfig = new LevelTwoActionBuilderConfig(levelTwoConfig);

    oldGenThresholdLevelOne = ConfigUtils.readConfig(configs, OLD_GEN_THRESHOLD_LEVEL_ONE_CONFIG_NAME, Double.class);
    if (oldGenThresholdLevelOne == null) {
      oldGenThresholdLevelOne = DEFAULT_OLD_GEN_THRESHOLD_LEVEL_ONE;
    }
    oldGenThresholdLevelTwo = ConfigUtils.readConfig(configs, OLD_GEN_THRESHOLD_LEVEL_TWO_CONFIG_NAME, Double.class);
    if (oldGenThresholdLevelTwo == null) {
      oldGenThresholdLevelTwo = DEFAULT_OLD_GEN_THRESHOLD_LEVEL_TWO;
    }
    oldGenThresholdLevelThree = ConfigUtils.readConfig(configs, OLD_GEN_THRESHOLD_LEVEL_THREE_CONFIG_NAME, Double.class);
    if (oldGenThresholdLevelThree == null) {
      oldGenThresholdLevelThree = DEFAULT_OLD_GEN_THRESHOLD_LEVEL_THREE;
    }
  }

  public LevelOneActionBuilderConfig levelOneActionBuilderConfig() {
    return levelOneActionBuilderConfig;
  }

  public LevelTwoActionBuilderConfig levelTwoActionBuilderConfig() {
    return levelTwoActionBuilderConfig;
  }

  public double oldGenThresholdLevelOne() {
    return oldGenThresholdLevelOne;
  }

  public double oldGenThresholdLevelTwo() {
    return oldGenThresholdLevelTwo;
  }

  public double oldGenThresholdLevelThree() {
    return oldGenThresholdLevelThree;
  }
}
