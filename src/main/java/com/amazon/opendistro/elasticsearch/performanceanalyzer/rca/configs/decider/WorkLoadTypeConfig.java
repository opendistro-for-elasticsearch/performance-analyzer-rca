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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.ConfigUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class WorkLoadTypeConfig {

  private static final String PRIORITY_ORDER_CONFIG_NAME = "priority-order";
  private static String INGEST = "ingest";
  private static String SEARCH = "search";
  public static final List<String> DEFAULT_PRIORITY_ORDER = Collections.unmodifiableList(
      Arrays.asList(INGEST, SEARCH));
  private List<String> priorityOrder;

  public WorkLoadTypeConfig(Map<String, Object> config) {
    priorityOrder = DEFAULT_PRIORITY_ORDER;
    List<String> workloadPriorityOrder = ConfigUtils.readConfig(config, PRIORITY_ORDER_CONFIG_NAME, List.class);
    if (workloadPriorityOrder != null || workloadPriorityOrder.size() >= 2) {
      if (INGEST.equals(workloadPriorityOrder.get(1)) && SEARCH.equals(workloadPriorityOrder.get(0))) {
        priorityOrder = Collections.unmodifiableList(
            Arrays.asList(SEARCH, INGEST));
      }
    }
  }

  public List<String> getPriorityOrder() {
    return priorityOrder;
  }

  public boolean preferIngestOverSearch() {
    return INGEST.equals(priorityOrder.get(0));
  }
}
