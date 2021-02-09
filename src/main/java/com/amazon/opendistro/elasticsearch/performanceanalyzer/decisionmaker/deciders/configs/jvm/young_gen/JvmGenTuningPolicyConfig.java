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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm.young_gen;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.JvmGenTuningPolicy;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Config;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NestedConfig;
import java.util.concurrent.TimeUnit;

/**
 * Configures various options for the
 * {@link JvmGenTuningPolicy}
 *
 * <p>The config follows the format below
 *  "decider-config-settings": {
 *    "jvm-gen-tuning-policy-config": {
 *       "enabled": true,
 *       "allow-young-gen-downsize": false,
 *       "day-breach-threshold": 5,
 *       "week-breach-threshold": 7,
 *       "day-monitor-window-size-minutes": 1440
 *       "week-monitor-window-size-minutes": 5760
 *       "day-monitor-bucket-size-minutes": 30
 *       "week-monitor-bucket-size-minutes": 1440
 *       ...
 *    }
 * }
 */
public class JvmGenTuningPolicyConfig {
  private static final String ENABLED = "enabled";
  private static final String ALLOW_YOUNG_GEN_DOWNSIZE = "allow-young-gen-downsize";
  private static final String DAY_BREACH_THRESHOLD = "day-breach-threshold";
  private static final String WEEK_BREACH_THRESHOLD = "week-breach-threshold";
  private static final String DAY_MONITOR_WINDOW_SIZE_MINUTES = "day-monitor-window-size-minutes";
  private static final String WEEK_MONITOR_WINDOW_SIZE_MINUTES = "week-monitor-window-size-minutes";
  private static final String DAY_MONITOR_BUCKET_SIZE_MINUTES = "day-monitor-bucket-size-minutes";
  private static final String WEEK_MONITOR_BUCKET_SIZE_MINUTES = "week-monitor-bucket-size-minutes";

  public static final boolean DEFAULT_ENABLED = true;
  public static final boolean DEFAULT_ALLOW_YOUNG_GEN_DOWNSIZE = false;
  public static final int DEFAULT_DAY_BREACH_THRESHOLD = 5;
  public static final int DEFAULT_WEEK_BREACH_THRESHOLD = 2;
  public static final int DEFAULT_DAY_MONITOR_WINDOW_SIZE_MINUTES = (int) TimeUnit.DAYS.toMinutes(1);
  public static final int DEFAULT_WEEK_MONITOR_WINDOW_SIZE_MINUTES = (int) TimeUnit.DAYS.toMinutes(4);
  public static final int DEFAULT_DAY_MONITOR_BUCKET_SIZE_MINUTES = 30;
  public static final int DEFAULT_WEEK_MONITOR_BUCKET_SIZE_MINUTES = (int) TimeUnit.DAYS.toMinutes(1);

  private Config<Integer> dayBreachThreshold;
  private Config<Integer> weekBreachThreshold;
  private Config<Boolean> enabled;
  private Config<Boolean> allowYoungGenDownsize;
  private Config<Integer> dayMonitorWindowSizeMinutes;
  private Config<Integer> weekMonitorWindowSizeMinutes;
  private Config<Integer> dayMonitorBucketSizeMinutes;
  private Config<Integer> weekMonitorBucketSizeMinutes;

  public JvmGenTuningPolicyConfig(NestedConfig config) {
    enabled = new Config<>(ENABLED, config.getValue(), DEFAULT_ENABLED, Boolean.class);
    allowYoungGenDownsize = new Config<>(ALLOW_YOUNG_GEN_DOWNSIZE, config.getValue(),
        DEFAULT_ALLOW_YOUNG_GEN_DOWNSIZE, Boolean.class);
    dayBreachThreshold = new Config<>(DAY_BREACH_THRESHOLD, config.getValue(),
        DEFAULT_DAY_BREACH_THRESHOLD, Integer.class);
    weekBreachThreshold = new Config<>(WEEK_BREACH_THRESHOLD, config.getValue(),
        DEFAULT_WEEK_BREACH_THRESHOLD, Integer.class);
    dayMonitorWindowSizeMinutes = new Config<>(DAY_MONITOR_WINDOW_SIZE_MINUTES, config.getValue(),
        DEFAULT_DAY_MONITOR_WINDOW_SIZE_MINUTES, Integer.class);
    weekMonitorWindowSizeMinutes = new Config<>(WEEK_MONITOR_WINDOW_SIZE_MINUTES, config.getValue(),
        DEFAULT_WEEK_MONITOR_WINDOW_SIZE_MINUTES, Integer.class);
    dayMonitorBucketSizeMinutes = new Config<>(DAY_MONITOR_BUCKET_SIZE_MINUTES, config.getValue(),
        DEFAULT_DAY_MONITOR_BUCKET_SIZE_MINUTES, Integer.class);
    weekMonitorBucketSizeMinutes = new Config<>(WEEK_MONITOR_BUCKET_SIZE_MINUTES, config.getValue(),
        DEFAULT_WEEK_MONITOR_BUCKET_SIZE_MINUTES, Integer.class);
  }

  /**
   * Whether or not to enable the policy. A disabled policy will not emit any actions.
   * @return Whether or not to enable the policy
   */
  public boolean isEnabled() {
    return enabled.getValue();
  }

  /**
   * Whether or not the policy should suggest scaling down the young generation
   * @return Whether or not the policy should suggest scaling down the young generation
   */
  public boolean allowYoungGenDownsize() {
    return allowYoungGenDownsize.getValue();
  }

  public int getDayBreachThreshold() {
    return dayBreachThreshold.getValue();
  }

  public int getWeekBreachThreshold() {
    return weekBreachThreshold.getValue();
  }

  public int getDayMonitorWindowSizeMinutes() {
    return dayMonitorWindowSizeMinutes.getValue();
  }

  public int getWeekMonitorWindowSizeMinutes() {
    return weekMonitorWindowSizeMinutes.getValue();
  }

  public int getDayMonitorBucketSizeMinutes() {
    return dayMonitorBucketSizeMinutes.getValue();
  }

  public int getWeekMonitorBucketSizeMinutes() {
    return weekMonitorBucketSizeMinutes.getValue();
  }
}
