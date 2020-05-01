package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.calculators.ShardBasedTemperatureCalculator;

public class HeapAllocRateByShardTemperatureCalculator extends ShardBasedTemperatureCalculator {

  public HeapAllocRateByShardTemperatureCalculator() {
    super(Dimension.Heap_AllocRate);
  }
}
