package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature;

public enum TemperatureDimension {
        CPU_Utilization(com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization.NAME),
        Heap_AllocRate(com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_AllocRate.NAME),
        Shard_Size_In_Bytes(com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.ShardSizeInBytes.NAME);

        public final String NAME;

        TemperatureDimension(String name) {
            this.NAME = name;
        }
}
