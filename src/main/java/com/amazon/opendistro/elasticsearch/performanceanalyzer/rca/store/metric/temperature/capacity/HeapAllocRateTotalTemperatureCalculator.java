package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.calculators.TotalNodeTemperatureCalculator;

public class HeapAllocRateTotalTemperatureCalculator extends TotalNodeTemperatureCalculator {

  public HeapAllocRateTotalTemperatureCalculator() {
    super(TemperatureDimension.Heap_AllocRate);
  }
}
