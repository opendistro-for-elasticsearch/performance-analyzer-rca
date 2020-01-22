package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.NamedAggregateValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.Value;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.MeasurementSet;
import java.util.HashMap;
import java.util.Map;

public class BaseFormatter implements Formatter {
  private Map<MeasurementSet, Map<Statistics, Value>> map;
  private long start;
  private long end;

  public BaseFormatter() {
    this.map = new HashMap<>();
    this.start = 0;
    this.end = 0;
  }

  @Override
  public void formatNamedAggregatedValue(
      MeasurementSet measurementSet, Statistics aggregationType, String name, Number value) {
    map.putIfAbsent(measurementSet, new HashMap<>());
    map.get(measurementSet)
        .put(aggregationType, new NamedAggregateValue(value, aggregationType, name));
  }

  @Override
  public void formatAggregatedValue(
      MeasurementSet measurementSet, Statistics aggregationType, Number value) {
    map.putIfAbsent(measurementSet, new HashMap<>());
    map.get(measurementSet).put(aggregationType, new Value(value));
  }

  @Override
  public void setStartAndEndTime(long start, long end) {
    this.start = start;
    this.end = end;
  }

  public Map<MeasurementSet, Map<Statistics, Value>> getFormatted() {
    return map;
  }
}
