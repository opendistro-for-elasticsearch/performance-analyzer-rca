package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.AggregateValue;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Count implements StatisticImpl<AggregateValue> {
  private AtomicLong counter;
  private boolean empty;

  public Count() {
    counter = new AtomicLong(0L);
    empty = true;
  }

  @Override
  public Statistics type() {
    return Statistics.COUNT;
  }

  @Override
  public void calculate(String key, Number value) {
    counter.incrementAndGet();
    empty = false;
  }

  @Override
  public List<AggregateValue> get() {
    return Collections.singletonList(new AggregateValue(counter, type()));
  }

  @Override
  public boolean empty() {
    return empty;
  }
}
