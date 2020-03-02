package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.samplers;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaRuntimeMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.collectors.SampleAggregator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.emitters.ISampler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;

public class RcaEnabledSampler implements ISampler {

  @Override
  public void sample(SampleAggregator sampleCollector) {
    sampleCollector.updateStat(RcaRuntimeMetrics.RCA_ENABLED, "", isRcaEnabled());
  }

  private int isRcaEnabled() {
    NodeDetails currentNode = ClusterDetailsEventProcessor.getCurrentNodeDetails();
    if (currentNode != null && currentNode.getIsMasterNode()) {
      return RcaController.isRcaEnabled() ? 1 : 0;
    }

    return 0;
  }
}
