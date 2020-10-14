package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.OLD_GEN_HEAP_USAGE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.UsageBucket;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import org.junit.Assert;
import org.junit.Test;

public class HeapBasedDeciderTest {
  private HeapBasedDecider createClusterRcaWithOldGenVal(double oldGenValue, NodeKey nodeKey) {
    RcaTestHelper<HotNodeSummary> nodeRca = new RcaTestHelper<>("QueueRejectionNodeRca");
    nodeRca.setAppContext(new AppContext());
    HighHeapUsageClusterRca highHeapUsageClusterRca = new HighHeapUsageClusterRca(1, nodeRca);

    ResourceContext context = new ResourceContext(Resources.State.UNHEALTHY);

    HotNodeSummary nodeSummary = new HotNodeSummary(nodeKey.getNodeId(), nodeKey.getHostAddress());
    nodeSummary.appendNestedSummary(new HotResourceSummary(OLD_GEN_HEAP_USAGE, 60, oldGenValue, 60));
    HotClusterSummary clusterSummary = new HotClusterSummary(1, 1);

    clusterSummary.appendNestedSummary(nodeSummary);

    highHeapUsageClusterRca.setLocalFlowUnit(new ResourceFlowUnit<>(System.currentTimeMillis(), context, clusterSummary, true));
    HeapBasedDecider heapBasedDecider = new HeapBasedDecider(1, 1, highHeapUsageClusterRca) {
      @Override
      public String name() {
        return null;
      }

      @Override
      public Decision operate() {
        return null;
      }
    };
    return heapBasedDecider;
  }

  @Test
  public void canUseMoreHeap() {
    NodeKey nodeKey = new NodeKey(new InstanceDetails.Id("xyz"), new InstanceDetails.Ip("1.1.1.1"));

    double percent = HeapBasedDecider.DEFAULT_HEAP_USAGE_THRESHOLDS.get(UsageBucket.UNDER_UTILIZED);
    double ratio = percent / 100.0;
    HeapBasedDecider heapBasedDecider = createClusterRcaWithOldGenVal(ratio, nodeKey);
    Assert.assertTrue(heapBasedDecider.canUseMoreHeap(nodeKey));

    percent = HeapBasedDecider.DEFAULT_HEAP_USAGE_THRESHOLDS.get(UsageBucket.HEALTHY_WITH_BUFFER);
    ratio = percent / 100.0;
    heapBasedDecider = createClusterRcaWithOldGenVal(ratio, nodeKey);
    Assert.assertTrue(heapBasedDecider.canUseMoreHeap(nodeKey));

    percent = HeapBasedDecider.DEFAULT_HEAP_USAGE_THRESHOLDS.get(UsageBucket.HEALTHY);
    ratio = percent / 100.0;
    heapBasedDecider = createClusterRcaWithOldGenVal(ratio, nodeKey);
    Assert.assertFalse(heapBasedDecider.canUseMoreHeap(nodeKey));

    percent = HeapBasedDecider.DEFAULT_HEAP_USAGE_THRESHOLDS.get(UsageBucket.HEALTHY) + 10;
    ratio = percent / 100.0;
    heapBasedDecider = createClusterRcaWithOldGenVal(ratio, nodeKey);
    Assert.assertFalse(heapBasedDecider.canUseMoreHeap(nodeKey));
  }
}