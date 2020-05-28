package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.calculators.AvgShardBasedTemperatureCalculator;

public class ShardSizeAvgTemperatureCalculator extends
    AvgShardBasedTemperatureCalculator {

  public ShardSizeAvgTemperatureCalculator() {
    super(Dimension.Shard_Size);
  }
}
