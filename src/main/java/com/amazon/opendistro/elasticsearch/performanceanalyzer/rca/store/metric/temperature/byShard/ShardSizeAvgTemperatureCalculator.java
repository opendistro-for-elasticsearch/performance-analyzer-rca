package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.calculators.AvgShardBasedTemperatureCalculator;

/**
*Class for returning the Average over the sizes of different Shards held by the node.
*/
public class ShardSizeAvgTemperatureCalculator extends
    AvgShardBasedTemperatureCalculator {

  public ShardSizeAvgTemperatureCalculator() {
    super(TemperatureDimension.Shard_Size_In_Bytes);
  }
}
