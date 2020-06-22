package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.temperature.DimensionalTemperatureFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.ShardStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector.VectorValues;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.HeapAllocRateByShardAvgTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.HeapAllocRateByShardTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.HeapAllocRateTotalTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent.HeapAllocRateShardIndependentTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.DimensionalTemperatureCalculator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HeapAllocRateTemperatureRca extends Rca<DimensionalTemperatureFlowUnit> {

  private static final Logger LOG = LogManager.getLogger(HeapAllocRateTemperatureRca.class);
  // The threshold set here is an initial threshold only.
  // TODO: Update the threshold appropriately after testing so that we assign heat correctly.
  // Raw Values here is not needed as we would be comparing just against the threshold.
  private static final TemperatureVector.VectorValues THRESHOLD_NORMALIZED_VAL_FOR_HEAT_ZONE_ASSIGNMENT =
          new TemperatureVector.VectorValues((short) 2, 0);
  private final HeapAllocRateByShardTemperatureCalculator HEAP_ALLOC_RATE_BY_SHARD;
  private final HeapAllocRateByShardAvgTemperatureCalculator HEAP_ALLOC_RATE_BY_SHARD_AVG;
  private final HeapAllocRateShardIndependentTemperatureCalculator HEAP_ALLOC_RATE_SHARD_INDEPENDENT;
  private final HeapAllocRateTotalTemperatureCalculator HEAP_ALLOC_RATE_TOTAL;
  private final ShardStore SHARD_STORE;

  public HeapAllocRateTemperatureRca(final long evaluationIntervalSeconds,
                                     final ShardStore shardStore,
                                     final HeapAllocRateByShardTemperatureCalculator heapAllocByShard,
                                     final HeapAllocRateByShardAvgTemperatureCalculator heapAllocByShardAvg,
                                     final HeapAllocRateShardIndependentTemperatureCalculator shardIndependentHeapAllocRate,
                                     final HeapAllocRateTotalTemperatureCalculator heapAllocRateTotal) {
    super(evaluationIntervalSeconds);
    this.SHARD_STORE = shardStore;
    this.HEAP_ALLOC_RATE_BY_SHARD = heapAllocByShard;
    this.HEAP_ALLOC_RATE_BY_SHARD_AVG = heapAllocByShardAvg;
    this.HEAP_ALLOC_RATE_SHARD_INDEPENDENT = shardIndependentHeapAllocRate;
    this.HEAP_ALLOC_RATE_TOTAL = heapAllocRateTotal;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new IllegalStateException("This node: [" + name() + "] should not have received flow "
        + "units from remote nodes.");
  }

  @Override
  public DimensionalTemperatureFlowUnit operate() {
      LOG.debug("executing : {}", name());
      DimensionalTemperatureFlowUnit heapAllocRateTemperatureFlowUnit = DimensionalTemperatureCalculator.getTemperatureForDimension(
              SHARD_STORE,
              TemperatureDimension.Heap_AllocRate,
              HEAP_ALLOC_RATE_BY_SHARD,
              HEAP_ALLOC_RATE_BY_SHARD_AVG,
              HEAP_ALLOC_RATE_SHARD_INDEPENDENT,
              HEAP_ALLOC_RATE_TOTAL,
              THRESHOLD_NORMALIZED_VAL_FOR_HEAT_ZONE_ASSIGNMENT);
      LOG.error("Heap allocation rate temperature calculated: {}",
              heapAllocRateTemperatureFlowUnit.getNodeDimensionProfile().toJson());
      return heapAllocRateTemperatureFlowUnit;
  }
}
