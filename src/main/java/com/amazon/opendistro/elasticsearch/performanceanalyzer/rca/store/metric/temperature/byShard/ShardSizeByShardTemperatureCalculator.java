package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.calculators.ShardBasedTemperatureCalculator;

/*
Class for returning the Shard Size Metric for individual shards held by the node.
*/
public class ShardSizeByShardTemperatureCalculator extends ShardBasedTemperatureCalculator {

    public ShardSizeByShardTemperatureCalculator() {
        super(Dimension.Shard_Size_In_Bytes);
    }
}
