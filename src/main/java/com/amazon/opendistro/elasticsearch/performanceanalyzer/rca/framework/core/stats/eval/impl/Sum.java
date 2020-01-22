package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.AggregateValue;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Sum implements StatisticImpl<AggregateValue> {
  private AtomicLong sum;
  private boolean empty;

  public Sum() {
    sum = new AtomicLong(0L);
    empty = true;
  }

  @Override
  public Statistics type() {
    return Statistics.SUM;
  }

  @Override
  public void calculate(String key, Number value) {
    sum.addAndGet(value.longValue());
    empty = false;
  }

  @Override
  public List<AggregateValue> get() {
    return Collections.singletonList(new AggregateValue(sum, type()));
  }

  @Override
  public boolean isEmpty() {
    return empty;
  }
}
