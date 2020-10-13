package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.BasicBucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.BucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.UsageBucket;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.ImmutableMap;

public abstract class HeapBasedDecider extends Decider {
  private HighHeapUsageClusterRca highHeapUsageClusterRca;
  private static final String OLD_GEN_TUNABLE_KEY = "old-gen";
  private static final ResourceEnum DECIDING_HEAP_RESOURCE_TYPE = ResourceEnum.OLD_GEN;
  public static final ImmutableMap<UsageBucket, Double> HEAP_USAGE_MAP = ImmutableMap.<UsageBucket, Double>builder()
      .put(UsageBucket.UNDER_UTILIZED, 10.0)
      .put(UsageBucket.HEALTHY_WITH_BUFFER, 60.0)
      .put(UsageBucket.HEALTHY, 80.0)
      .build();

  public HeapBasedDecider(long evalIntervalSeconds, int decisionFrequency, HighHeapUsageClusterRca highHeapUsageClusterRca) {
    super(evalIntervalSeconds, decisionFrequency);
    this.highHeapUsageClusterRca = highHeapUsageClusterRca;
  }

  /**
   * The Queue and Cache deciders should only be able to suggest increase of the queue size or increase of cache size if the Java heap can
   * sustain more live objects in it without de-gradation. What is an acceptable heap usage limit to determine this, comes from the
   * bucketization object in rca.conf. We compare the oldGen usage percent reported by the HighHeapUsage RCA to determine that.
   *
   * @param esNode The EsNode we are trying to make a decision for.
   * @return return if the OldGen heap is under-utilized or healthy and yet more can be consumed, return true; or false otherwise.
   */
  protected boolean canUseMoreHeap(NodeKey esNode) {
    // we add action only if heap is under-utilized or healthy and yet more can be consumed.
    for (ResourceFlowUnit<HotClusterSummary> clusterSummary : highHeapUsageClusterRca.getFlowUnits()) {
      if (clusterSummary.hasResourceSummary()) {
        for (HotNodeSummary nodeSummary : clusterSummary.getSummary().getHotNodeSummaryList()) {
          NodeKey thisNode = new NodeKey(nodeSummary.getNodeID(), nodeSummary.getHostAddress());
          if (thisNode.equals(esNode)) {
            for (HotResourceSummary hotResourceSummary : nodeSummary.getHotResourceSummaryList()) {
              Resource resource = hotResourceSummary.getResource();
              if (resource.getResourceEnum() == DECIDING_HEAP_RESOURCE_TYPE) {
                double oldGenUsedRatio = hotResourceSummary.getValue();
                double oldGenUsedPercent = oldGenUsedRatio * 100;
                BucketCalculator bucketCalculator;
                try {
                  bucketCalculator = rcaConf.getBucketizationSettings(OLD_GEN_TUNABLE_KEY);
                } catch (Exception jsonEx) {
                  bucketCalculator = new BasicBucketCalculator(HEAP_USAGE_MAP);
                }
                UsageBucket bucket = bucketCalculator.compute(oldGenUsedPercent);
                if (bucket == UsageBucket.UNDER_UTILIZED || bucket == UsageBucket.HEALTHY_WITH_BUFFER) {
                  return true;
                } else {
                  return false;
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
