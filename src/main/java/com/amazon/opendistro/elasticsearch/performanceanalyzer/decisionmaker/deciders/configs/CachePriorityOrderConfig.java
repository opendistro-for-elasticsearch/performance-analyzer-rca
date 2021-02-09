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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Config;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NestedConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * "cache-type": {
 *    "priority-order": ["fielddata-cache", "shard-request-cache"]
 * }
 */
public class CachePriorityOrderConfig {
  private static final String PRIORITY_ORDER_CONFIG_NAME = "priority-order";
  private static String FIELDDATA_CACHE = "fielddata-cache";
  private static String SHARD_REQUEST_CACHE = "shard-request-cache";
  public static final List<String> DEFAULT_PRIORITY_ORDER = Collections.unmodifiableList(
      Arrays.asList(FIELDDATA_CACHE, SHARD_REQUEST_CACHE));
  private Config<List<String>> priorityOrder;

  public CachePriorityOrderConfig(NestedConfig configs) {
    priorityOrder = new Config(PRIORITY_ORDER_CONFIG_NAME, configs.getValue(),
        DEFAULT_PRIORITY_ORDER, List.class);
  }

  public List<String> getPriorityOrder() {
    return priorityOrder.getValue();
  }
}
