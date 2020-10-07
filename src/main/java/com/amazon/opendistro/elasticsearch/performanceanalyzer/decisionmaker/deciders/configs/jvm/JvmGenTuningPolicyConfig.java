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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.JvmGenTuningPolicy;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Config;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NestedConfig;

/**
 * Configures various options for the
 * {@link JvmGenTuningPolicy}
 *
 * <p>The config follows the format below
 *  "decider-config-settings": {
 *    "jvm-generation-tuning-policy-config": {
 *       "enabled": true,
 *       "allow-young-gen-downsize": false,
 *       ...
 *    }
 * }
 */
public class JvmGenTuningPolicyConfig {
  private static final String ENABLED = "enabled";
  private static final String ALLOW_YOUNG_GEN_DOWNSIZE = "allow-young-gen-downsize";

  public static final boolean DEFAULT_ENABLED = false;
  public static final boolean DEFAULT_ALLOW_YOUNG_GEN_DOWNSIZE = false;

  private Config<Boolean> enabled;
  private Config<Boolean> allowYoungGenDownsize;

  public JvmGenTuningPolicyConfig(NestedConfig config) {
    enabled = new Config<>(ENABLED, config.getValue(), DEFAULT_ENABLED, Boolean.class);
    allowYoungGenDownsize = new Config<>(ALLOW_YOUNG_GEN_DOWNSIZE, config.getValue(),
        DEFAULT_ALLOW_YOUNG_GEN_DOWNSIZE, Boolean.class);
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
}
