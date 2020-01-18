package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.NamedAggregateValue;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NamedCounter implements StatisticImpl<NamedAggregateValue> {
  private boolean empty;
  private Map<String, NamedAggregateValue> counters;

  public NamedCounter() {
    counters = new ConcurrentHashMap<>();
    empty = true;
  }

  @Override
  public Statistics type() {
    return Statistics.NAMED_COUNTERS;
  }

  @Override
  public void calculate(String key, Number value) {
    synchronized (this) {
      NamedAggregateValue mapValue =
          counters.getOrDefault(key, new NamedAggregateValue(0L, Statistics.NAMED_COUNTERS, key));
      long number = mapValue.getValue().longValue();
      long newNumber = number + 1;
      mapValue.update(newNumber);
      counters.put(key, mapValue);
      empty = false;
    }
  }

  @Override
  public Collection<NamedAggregateValue> get() {
    return counters.values();
  }

  @Override
  public boolean empty() {
    return empty;
  }
}
