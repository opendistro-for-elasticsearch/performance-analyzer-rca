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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Config;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NestedConfig;

/**
 * "workload-type": {
 *   "prefer-ingest": true,
 *   "prefer-search": false
 * }
 */
public class WorkLoadTypeConfig {
  private static final String INGEST_CONFIG = "prefer-ingest";
  private static final String SEARCH_CONFIG = "prefer-search";
  public static final boolean DEFAULT_PREFER_INGEST = false;
  public static final boolean DEFAULT_PREFER_SEARCH = false;
  private Config<Boolean> preferIngest;
  private Config<Boolean> preferSearch;

  public WorkLoadTypeConfig(NestedConfig configs) {
    preferIngest = new Config<>(INGEST_CONFIG, configs.getValue(),
        DEFAULT_PREFER_INGEST, Boolean.class);
    preferSearch = new Config<>(SEARCH_CONFIG, configs.getValue(),
        DEFAULT_PREFER_SEARCH, Boolean.class);
  }

  public boolean preferIngest() {
    return preferIngest.getValue();
  }

  public boolean preferSearch() {
    return preferSearch.getValue();
  }
}
