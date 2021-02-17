/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;
import java.util.Collections;
import java.util.List;

/**
 * metrics added by each RCA vertex in RCA graph. All metrics under this category are
 * RCA specific
 */
public enum RcaVerticesMetrics implements MeasurementSet {
  NUM_YOUNG_GEN_RCA_TRIGGERED(
      "YoungGenRcaCount", "count", Collections.singletonList(Statistics.COUNT)),
  NUM_OLD_GEN_RCA_TRIGGERED(
      "OldGenRcaCount", "count", Collections.singletonList(Statistics.COUNT)),
  NUM_HIGH_HEAP_CLUSTER_RCA_TRIGGERED(
      "HighHeapClusterRcaCount", "count", Collections.singletonList(Statistics.COUNT)),
  YOUNG_GEN_RCA_NAMED_COUNT(
      "YoungGenRcaNamedCount", "namedCount", Collections.singletonList(Statistics.NAMED_COUNTERS)),
  NUM_FIELD_DATA_CACHE_RCA_TRIGGERED(
      "FieldDataCacheRcaCount", "count", Collections.singletonList(Statistics.COUNT)),
  NUM_SHARD_REQUEST_CACHE_RCA_TRIGGERED(
          "ShardRequestCacheCount", "count", Collections.singletonList(Statistics.COUNT)),
  CLUSTER_RCA_NAMED_COUNT(
          "ClusterRcaNamedCount", "namedCount", Collections.singletonList(Statistics.NAMED_COUNTERS));

  /** What we want to appear as the metric name. */
  private String name;

  /**
   * The unit the measurement is in. This is not used for the statistics calculations but as an
   * information that will be dumped with the metrics.
   */
  private String unit;

  /**
   * Multiple statistics can be collected for each measurement like MAX, MIN and MEAN. This is a
   * collection of one or more such statistics.
   */
  private List<Statistics> statsList;

  RcaVerticesMetrics(String name, String unit, List<Statistics> statisticList) {
    this.name = name;
    this.unit = unit;
    this.statsList = statisticList;
  }

  public String toString() {
    return new StringBuilder(name).append("-").append(unit).toString();
  }

  @Override
  public List<Statistics> getStatsList() {
    return statsList;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getUnit() {
    return unit;
  }
}
