package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.MeasurementSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsCollectorFormatter implements Formatter {
  StatsCollectorReturn formattedValue;
  static final char SEPARATOR = '-';

  public StatsCollectorReturn getFormatted() {
    return formattedValue;
  }

  @Override
  public void formatNamedAggregatedValue(
      MeasurementSet measurementSet, Statistics aggregationType, String name, Number value) {
    formattedValue.putLatencies(
        new StringBuilder(measurementSet.getName())
            .append(SEPARATOR)
            .append(aggregationType)
            .append(SEPARATOR)
            .append(name)
            .toString(),
        value.doubleValue());
    formattedValue.getStatsdata().putIfAbsent(measurementSet.getName(), measurementSet.getUnit());
  }

  @Override
  public void formatNamedValue(MeasurementSet measurementSet, String name, Number value) {
    formattedValue.putCounter(
        new StringBuilder(measurementSet.getName()).append(SEPARATOR).append(name).toString(),
        value.intValue());
    formattedValue.getStatsdata().putIfAbsent(measurementSet.getName(), measurementSet.getUnit());
  }

  @Override
  public void formatAggregatedValue(
      MeasurementSet measurementSet, Statistics aggregationType, Number value) {
    formattedValue.putLatencies(
        new StringBuilder(measurementSet.getName()).append(SEPARATOR).append(aggregationType).toString(),
        value.doubleValue());
    formattedValue.getStatsdata().putIfAbsent(measurementSet.getName(), measurementSet.getUnit());
  }

  @Override
  public void formatValue(MeasurementSet measurementSet, Number value) {
    formattedValue.putCounter(measurementSet.getName(), value.intValue());
    formattedValue.getStatsdata().putIfAbsent(measurementSet.getName(), measurementSet.getUnit());
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
  }
}
