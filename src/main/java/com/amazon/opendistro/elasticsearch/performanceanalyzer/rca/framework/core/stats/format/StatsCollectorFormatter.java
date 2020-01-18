/*
 *  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.MeasurementSet;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.aggregated.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.aggregated.RcaFrameworkMeasurements;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.aggregated.RcaGraphMeasurements;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsCollectorFormatter implements Formatter {
  static final char SEPARATOR = '-';
  static List<MeasurementSet> LATENCIES =
      Arrays.asList(
          RcaFrameworkMeasurements.GRAPH_EXECUTION_TIME,
          RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL,
          RcaGraphMeasurements.METRIC_GATHER_CALL,
          RcaGraphMeasurements.RCA_PERSIST_CALL);
  StatsCollectorReturn formattedValue;

  public StatsCollectorReturn getFormatted() {
    return formattedValue;
  }

  private boolean formatException(MeasurementSet measurement, String name, Number counter) {
    for (MeasurementSet measure : ExceptionsAndErrors.values()) {
      if (measurement == measure) {
        if (name.isEmpty()) {
          formattedValue.putCounter(measurement.getName(), counter.intValue());
        } else {
          formattedValue.putCounter(
              new StringBuilder(measurement.getName())
                  .append(SEPARATOR)
                  .append(name.replaceAll(" ", "_"))
                  .toString(),
              counter.intValue());
        }
        return true;
      }
    }
    return false;
  }

  private boolean formatSamples(MeasurementSet given, Number values, Statistics aggr) {
    if (aggr == Statistics.SAMPLE) {
      formattedValue.putStats(given.getName(), String.valueOf(values));
      return true;
    }
    return false;
  }

  private boolean formatCounters(
      MeasurementSet measurementSet, Number values, Statistics aggr, String name) {
    boolean found = false;
    if (aggr == Statistics.COUNT) {
      formattedValue.putCounter(measurementSet.getName(), values.intValue());
      found = true;
    } else if (aggr == Statistics.NAMED_COUNTERS) {
      formattedValue.putCounter(
          new StringBuilder(measurementSet.getName())
              .append(SEPARATOR)
              .append(name.replaceAll(" ", "_"))
              .toString(),
          values.intValue());
      found = true;
    }
    return found;
  }

  private boolean formatLatencies(
      MeasurementSet given, Number value, Statistics aggr, String name) {
    if (aggr == Statistics.NAMED_COUNTERS
        || aggr == Statistics.NAMED_COUNTERS
        || aggr == Statistics.SAMPLE) {
      return false;
    }
    for (MeasurementSet aggregateMeasurements : LATENCIES) {
      if (aggregateMeasurements == given) {
        StringBuilder stringBuilder = new StringBuilder(given.getName());
        stringBuilder.append(SEPARATOR).append(aggr);
        stringBuilder.append(SEPARATOR).append(given.getUnit());
        if (!name.isEmpty()) {
          stringBuilder.append(SEPARATOR).append(name);
        }
        formattedValue.putLatencies(stringBuilder.toString(), value.doubleValue());
        return true;
      }
    }
    return false;
  }

  private void formatStats(MeasurementSet measurement, Number value, Statistics aggr, String name) {
    StringBuilder stringBuilder = new StringBuilder(measurement.getName());
    stringBuilder.append(SEPARATOR).append(aggr);
    stringBuilder.append(SEPARATOR).append(measurement.getUnit());
    if (!name.isEmpty()) {
      stringBuilder.append(SEPARATOR).append(name);
    }
    formattedValue.putStats(stringBuilder.toString(), String.valueOf(value));
  }

  private void formatMeasurementInOrder(
      MeasurementSet measurementSet, Statistics type, String name, Number value) {
    boolean ret = formatSamples(measurementSet, value, type);
    if (!ret) {
      ret = formatException(measurementSet, name, value);
    }
    if (!ret) {
      ret = formatCounters(measurementSet, value, type, name);
    }
    if (!ret) {
      ret = formatLatencies(measurementSet, value, type, name);
    }
    if (!ret) {
      formatStats(measurementSet, value, type, name);
    }
  }

  @Override
  public void formatNamedAggregatedValue(
      MeasurementSet measurementSet, Statistics aggregationType, String name, Number value) {
    formatMeasurementInOrder(measurementSet, aggregationType, name, value);
  }

  @Override
  public void formatAggregatedValue(
      MeasurementSet measurementSet, Statistics aggregationType, Number value) {
    formatMeasurementInOrder(measurementSet, aggregationType, "", value);
  }

  @Override
  public void setStartAndEndTime(long start, long end) {
    formattedValue = new StatsCollectorReturn(start, end);
  }

  public class StatsCollectorReturn {
    private Map<String, AtomicInteger> counters;
    private Map<String, String> statsdata;
    private Map<String, Double> latencies;
    private long startTimeMillis;
    private long endTimeMillis;

    public StatsCollectorReturn(long startTimeMillis, long endTimeMillis) {
      counters = new HashMap<>();
      statsdata = new HashMap<>();
      latencies = new HashMap<>();
      this.startTimeMillis = startTimeMillis;
      this.endTimeMillis = endTimeMillis;
    }

    void putCounter(String name, int value) {
      counters.put(name, new AtomicInteger(value));
    }

    void putLatencies(String name, double value) {
      latencies.put(name, value);
    }

    void putStats(String name, String value) {
      statsdata.put(name, value);
    }

    public Map<String, AtomicInteger> getCounters() {
      return counters;
    }

    public Map<String, String> getStatsdata() {
      return statsdata;
    }

    public Map<String, Double> getLatencies() {
      return latencies;
    }

    public long getStartTimeMillis() {
      return startTimeMillis;
    }

    public long getEndTimeMillis() {
      return endTimeMillis;
    }

    public boolean isEmpty() {
      return counters.isEmpty() && statsdata.isEmpty() && latencies.isEmpty();
    }
  }
}
