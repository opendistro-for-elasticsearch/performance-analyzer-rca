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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NestedConfig {

  private static final Logger LOG = LogManager.getLogger(NestedConfig.class);

  private String key;
  private Map<String, Object> value;

  public NestedConfig(String key, Map<String, Object> parentConfig) {
    this.key = key;
    this.value = null;
    if (parentConfig != null) {
      //noinspection unchecked
      value = (Map<String, Object>) parentConfig.get(key);
    }
  }

  public String getKey() {
    return key;
  }

  public Map<String, Object> getValue() {
    return value;
  }
}
