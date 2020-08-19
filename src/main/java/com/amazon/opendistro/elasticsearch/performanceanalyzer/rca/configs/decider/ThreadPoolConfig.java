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
import java.util.Map;

public class ThreadPoolConfig {
  public static final int DEFAULT_WRITE_QUEUE_CAPACITY_LOWER_BOUND = 100;
  public static final int DEFAULT_WRITE_QUEUE_CAPACITY_UPPER_BOUND = 1000;
  public static final int DEFAULT_SEARCH_QUEUE_CAPACITY_LOWER_BOUND = 1000;
  public static final int DEFAULT_SEARCH_QUEUE_CAPACITY_UPPER_BOUND = 3000;
  private static final String WRITE_QUEUE_CAPACITY_UPPER_BOUND = "write-queue-capacity-upper-bound";
  private static final String WRITE_QUEUE_CAPACITY_LOWER_BOUND = "write-queue-capacity-lower-bound";
  private static final String SEARCH_QUEUE_CAPACITY_UPPER_BOUND = "search-queue-capacity-upper-bound";
  private static final String SEARCH_QUEUE_CAPACITY_LOWER_BOUND = "search-queue-capacity-lower-bound";
  private Integer writeQueueCapacityUpperBound;
  private Integer writeQueueCapacityLowerBound;
  private Integer searchQueueCapacityUpperBound;
  private Integer searchQueueCapacityLowerBound;

  public ThreadPoolConfig(Map<String, Object> configs) {
    writeQueueCapacityLowerBound = ConfigUtils
        .readConfig(configs, WRITE_QUEUE_CAPACITY_LOWER_BOUND, Integer.class);
    if (writeQueueCapacityLowerBound == null) {
      writeQueueCapacityLowerBound = DEFAULT_WRITE_QUEUE_CAPACITY_LOWER_BOUND;
    }
    writeQueueCapacityUpperBound = ConfigUtils
        .readConfig(configs, WRITE_QUEUE_CAPACITY_UPPER_BOUND, Integer.class);
    if (writeQueueCapacityUpperBound == null) {
      writeQueueCapacityUpperBound = DEFAULT_WRITE_QUEUE_CAPACITY_UPPER_BOUND;
    }
    searchQueueCapacityLowerBound = ConfigUtils
        .readConfig(configs, SEARCH_QUEUE_CAPACITY_LOWER_BOUND, Integer.class);
    if (searchQueueCapacityLowerBound == null) {
      searchQueueCapacityLowerBound = DEFAULT_SEARCH_QUEUE_CAPACITY_LOWER_BOUND;
    }
    searchQueueCapacityUpperBound = ConfigUtils
        .readConfig(configs, SEARCH_QUEUE_CAPACITY_UPPER_BOUND, Integer.class);
    if (searchQueueCapacityUpperBound == null) {
      searchQueueCapacityUpperBound = DEFAULT_SEARCH_QUEUE_CAPACITY_UPPER_BOUND;
    }
  }

  public int writeQueueCapacityLowerBound() {
    return writeQueueCapacityLowerBound;
  }

  public int writeQueueCapacityUpperBound() {
    return writeQueueCapacityUpperBound;
  }

  public int searchQueueCapacityLowerBound() {
    return searchQueueCapacityLowerBound;
  }

  public int searchQueueCapacityUpperBound() {
    return searchQueueCapacityUpperBound;
  }
}
