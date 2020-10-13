package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.UsageBucket;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;

public abstract class HeapBasedDecider extends Decider {
  private HighHeapUsageClusterRca highHeapUsageClusterRca;

  public HeapBasedDecider(long evalIntervalSeconds, int decisionFrequency, HighHeapUsageClusterRca highHeapUsageClusterRca) {
    super(evalIntervalSeconds, decisionFrequency);
    this.highHeapUsageClusterRca = highHeapUsageClusterRca;
  }

  protected boolean canUseMoreHeap(NodeKey esNode) {
    // we add action only if heap is under-utilized or healthy and yet more can be consumed.
    for (ResourceFlowUnit<HotClusterSummary> clusterSummary : highHeapUsageClusterRca.getFlowUnits()) {
      if (clusterSummary.hasResourceSummary()) {
        for (HotNodeSummary nodeSummary : clusterSummary.getSummary().getHotNodeSummaryList()) {
          NodeKey thisNode = new NodeKey(nodeSummary.getNodeID(), nodeSummary.getHostAddress());
          if (thisNode.equals(esNode)) {
            for (HotResourceSummary hotResourceSummary : nodeSummary.getHotResourceSummaryList()) {
              Resource resource = hotResourceSummary.getResource();
              if (resource.getResourceEnum() == ResourceEnum.OLD_GEN) {
                double oldGenUsedRatio = hotResourceSummary.getValue();
                double oldGenUsedPercent = oldGenUsedRatio * 100;
                UsageBucket bucket = rcaConf.getBucketizationSettings("old-gen").compute(oldGenUsedPercent);
                if (bucket == UsageBucket.HEALTHY || bucket == UsageBucket.UNHEALTHY) {
                  return false;
                } else {
                  return true;
                }
              }
            }
          }
        }
      }
    }
    return true;
  }
}
