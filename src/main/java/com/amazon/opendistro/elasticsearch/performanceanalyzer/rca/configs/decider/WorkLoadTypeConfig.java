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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Config;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NestedConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * "workload-type": {
 *   "priority-order": ["ingest", "search"]
 * }
 */
public class WorkLoadTypeConfig {

  private static final String PRIORITY_ORDER_CONFIG_NAME = "priority-order";
  private static String INGEST = "ingest";
  private static String SEARCH = "search";
  public static final List<String> DEFAULT_PRIORITY_ORDER = Collections.unmodifiableList(
      Arrays.asList(INGEST, SEARCH));
  private Config<List<String>> priorityOrder;
  private Predicate<List<String>> listValidator;

  public WorkLoadTypeConfig(NestedConfig configs) {
    listValidator = (list) -> {
      if (list.size() > 2) {
        return false;
      }
      if (SEARCH.equals(list.get(0))) {
        return INGEST.equals(list.get(1));
      }
      else if (INGEST.equals(list.get(0))) {
        return SEARCH.equals(list.get(1));
      }
      else {
        return false;
      }
    };
    priorityOrder = new Config(PRIORITY_ORDER_CONFIG_NAME, configs.getValue(),
        DEFAULT_PRIORITY_ORDER, listValidator, List.class);
  }

  public List<String> getPriorityOrder() {
    return priorityOrder.getValue();
  }

  public boolean preferIngestOverSearch() {
    return INGEST.equals(getPriorityOrder().get(0));
  }
}
