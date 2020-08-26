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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterSummary {
  private Map<String, HotClusterSummary> summaryMap;
  private Map<String, Long> timesUpdateMap;
  private long evaluationSecondinMillis;

  public ClusterSummary(long evaluationSecondinMillis, Map<String, HotClusterSummary> summaryMap) {
    this.evaluationSecondinMillis = evaluationSecondinMillis;
    this.timesUpdateMap = new HashMap<>();
    setSummaryMap(summaryMap);
  }

  public void setSummaryMap(Map<String, HotClusterSummary> summaryMap) {
    this.summaryMap = summaryMap;
    initiateTimesAddedMap();
  }

  public void initiateTimesAddedMap() {
    for (String key : summaryMap.keySet()) {
      timesUpdateMap.put(key, System.currentTimeMillis());
    }
  }

  public void addValidSummary(String name, HotClusterSummary summary, long timestamp) {
    summaryMap.put(name, summary);
    timesUpdateMap.put(name, timestamp);
    validateSummaryMap();
  }

  public void validateSummaryMap() {
    summaryMap.forEach((k, v) -> {
      if (v == null || System.currentTimeMillis() - timesUpdateMap.get(k) > evaluationSecondinMillis) {
        summaryMap.remove(k);
      }
    });
  }

  public long getLastUpdated(String name) {
    return timesUpdateMap.get(name);
  }

  public boolean summaryMapIsEmpty() {
    return summaryMap.isEmpty();
  }

  public Map<String, HotClusterSummary> getSummaryMap() {
    return summaryMap;
  }

  public List<String> getExistingClusterNameList() {
    if (!summaryMapIsEmpty()) {
      return new ArrayList<>(summaryMap.keySet());
    }
    return new ArrayList<>();
  }

  /**
   * Returns the latest summary for the given cluster name
   */
  public HotClusterSummary getSummary(String name) {
    return summaryMap.get(name);
  }
}
