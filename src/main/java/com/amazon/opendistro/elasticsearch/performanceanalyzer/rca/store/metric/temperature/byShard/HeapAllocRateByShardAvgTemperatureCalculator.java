package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.calculators.AvgShardBasedTemperatureCalculator;

public class HeapAllocRateByShardAvgTemperatureCalculator extends
    AvgShardBasedTemperatureCalculator {

  public HeapAllocRateByShardAvgTemperatureCalculator() {
    super(TemperatureDimension.Heap_AllocRate);
  }
}
