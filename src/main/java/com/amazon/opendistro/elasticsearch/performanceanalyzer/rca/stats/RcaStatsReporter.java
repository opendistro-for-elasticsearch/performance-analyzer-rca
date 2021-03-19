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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.collectors.SampleAggregator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.format.Formatter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;

/**
 * This is meant to be the registry for all the stats that are collected by the Rca framework and
 * needs to be reported periodically by the {@code StatsCollector.collectMetrics}
 */
public class RcaStatsReporter {
  /** The list of collectors for which a report can be generated. */
  private final List<SampleAggregator> aggregators;

  /** The index of the collector whose measurements will be reported on this iteration. */
  private int idxOfCollectorToReport;

  public RcaStatsReporter(List<SampleAggregator> aggregators) {
    this.aggregators = aggregators;
    idxOfCollectorToReport = 0;
  }

  /**
   * The caller is expected to call this method with a {@code new} formatter every time. This is not
   * thread-safe.
   *
   * <p>Each time this is called, this method sources a measurement type and formats it and sends
   * it.
   *
   * @param formatter The formatter to use to format the measurementSet
   * @return true if there are collectors left to be reported or false otherwise.
   */
  public boolean getNextReport(Formatter formatter) {
    if (aggregators == null || aggregators.isEmpty()) {
      return false;
    }

    SampleAggregator collector = aggregators.get(idxOfCollectorToReport);
    collector.fillValuesAndReset(formatter);
    boolean ret = true;

    idxOfCollectorToReport += 1;

    if (idxOfCollectorToReport == aggregators.size()) {
      ret = false;
      idxOfCollectorToReport = 0;
    }
    return ret;
  }

  @VisibleForTesting
  public boolean isMeasurementCollected(MeasurementSet measure) {
    for (SampleAggregator aggregator : aggregators) {
      if (aggregator.isMeasurementObserved(measure)) {
        return true;
      }
    }
    return false;
  }
}
