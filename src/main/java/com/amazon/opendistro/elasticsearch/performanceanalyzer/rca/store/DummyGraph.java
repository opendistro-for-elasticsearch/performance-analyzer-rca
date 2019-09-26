package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighShardCpuRca;
import java.util.Collections;

public class DummyGraph extends AnalysisGraph {
  @Override
  public void construct() {
    // Start will all the metrics we would need.
    Metric cpu = new CPU_Utilization(30);
    HighShardCpuRca highShardCpuRca = new HighShardCpuRca(60);
    addLeaf(cpu);
    highShardCpuRca.addAllUpstreams(Collections.singletonList(cpu));
  }
}
