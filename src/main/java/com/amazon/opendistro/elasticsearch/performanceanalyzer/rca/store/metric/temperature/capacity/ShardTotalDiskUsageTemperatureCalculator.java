package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.calculators.TotalNodeTemperatureCalculator;

/**
 * This class calculated the total disk used by shards in the node.
 */

public class ShardTotalDiskUsageTemperatureCalculator extends TotalNodeTemperatureCalculator {

    public ShardTotalDiskUsageTemperatureCalculator() {
        super(TemperatureVector.Dimension.Shard_Size_In_Bytes);
    }
}
