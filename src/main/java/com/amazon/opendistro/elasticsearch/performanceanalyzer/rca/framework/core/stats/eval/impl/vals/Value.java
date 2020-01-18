package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format.Formatter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.MeasurementSet;

public class Value {
  protected Number value;

  public Value(Number value) {
    this.value = value;
  }

  public Number getValue() {
    return value;
  }

  public void format(Formatter formatter, MeasurementSet measurementSet, Statistics stats) {
    formatter.formatValue(measurementSet, value);
  }
}
