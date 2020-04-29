package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent.calculators.ShardIndependentTemperatureCalculator;

public class HeapAllocRateShardIndependentTemperatureCalculator extends
    ShardIndependentTemperatureCalculator {

  public HeapAllocRateShardIndependentTemperatureCalculator() {
    super(Dimension.Heap_AllocRate);
  }
}
