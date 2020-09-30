package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.SizeUpJvmAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decision;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.jvmsizing.LargeHeapClusterRca;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JvmSizingDecider extends Decider {

  public static final String NAME = "JvmSizingDecider";
  private static final long EVAL_INTERVAL_IN_S = 5;

  private final PerNodeSlidingWindow perNodeSlidingWindow;

  private LargeHeapClusterRca largeHeapClusterRca;
  private int decisionFrequency;
  private int thresholdNumNodesPercent;
  private int unhealthyDataPointsThreshold;
  private int counter = 0;

  public JvmSizingDecider(int decisionFrequency, final LargeHeapClusterRca largeHeapClusterRca,
      int thresholdNumNodesPercent, int unhealthyDataPointsThreshold) {
    super(EVAL_INTERVAL_IN_S, decisionFrequency);
    this.largeHeapClusterRca = largeHeapClusterRca;
    this.decisionFrequency = decisionFrequency;
    this.thresholdNumNodesPercent = thresholdNumNodesPercent;
    this.unhealthyDataPointsThreshold = unhealthyDataPointsThreshold;
    this.perNodeSlidingWindow = new PerNodeSlidingWindow(4, TimeUnit.DAYS);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Decision operate() {
    long currTime = System.currentTimeMillis();
    ++counter;
    addToSlidingWindow();
    if (counter == decisionFrequency) {
      counter = 0;
      return evaluateAndEmit();
    }

    return new Decision(currTime, NAME);
  }

  private void addToSlidingWindow() {
    long currTime = System.currentTimeMillis();
    if (largeHeapClusterRca.getFlowUnits().isEmpty()) {
      return;
    }
    ResourceFlowUnit<HotClusterSummary> flowUnit = largeHeapClusterRca.getFlowUnits().get(0);

    if (flowUnit.getSummary() == null) {
      return;
    }
    List<HotNodeSummary> hotNodeSummaries = flowUnit.getSummary().getHotNodeSummaryList();
    hotNodeSummaries.forEach(hotNodeSummary -> {
      NodeKey nodeKey = new NodeKey(hotNodeSummary.getNodeID(), hotNodeSummary.getHostAddress());
      perNodeSlidingWindow.next(nodeKey, new SlidingWindowData(currTime, 1d));
    });
  }

  private Decision evaluateAndEmit() {
    Decision decision = new Decision(System.currentTimeMillis(), NAME);
    int numNodesInCluster = getAppContext().getAllClusterInstances().size();
    int numNodesInClusterUndersizedOldGen = getUnderSizedOldGenCount();

    if (numNodesInClusterUndersizedOldGen * 100 / numNodesInCluster >= thresholdNumNodesPercent) {
      decision.addAction(new SizeUpJvmAction(getAppContext()));
    }

    return decision;
  }

  /**
   * Gets the number of nodes that have had a significant number of unhealthy data points in the
   * last 96 hours.
   *
   * @return number of nodes that cross the threshold for unhealthy data points in the last 96
   * hours.
   */
  private int getUnderSizedOldGenCount() {
    int count = 0;
    for (NodeKey key : perNodeSlidingWindow.perNodeSlidingWindow.keySet()) {
      if (perNodeSlidingWindow.readCount(key) >= unhealthyDataPointsThreshold) {
        count++;
      }
    }

    return count;
  }

  private static class PerNodeSlidingWindow {
    private final int slidingWindowSize;
    private final TimeUnit windowSizeTimeUnit;
    private final Map<NodeKey, SlidingWindow<SlidingWindowData>> perNodeSlidingWindow;

    public PerNodeSlidingWindow(final int slidingWindowSize, final TimeUnit timeUnit) {
      this.slidingWindowSize = slidingWindowSize;
      this.windowSizeTimeUnit = timeUnit;
      this.perNodeSlidingWindow = new HashMap<>();
    }

    public void next(NodeKey node, SlidingWindowData data) {
      perNodeSlidingWindow.computeIfAbsent(node, n1 -> new SlidingWindow<>(slidingWindowSize,
          windowSizeTimeUnit)).next(data);
    }

    public int readCount(NodeKey node) {
      if (perNodeSlidingWindow.containsKey(node)) {
        SlidingWindow<SlidingWindowData> slidingWindow = perNodeSlidingWindow.get(node);
        double count = slidingWindow.readSum();
        return (int)count;
      }

      return 0;
    }
  }
}
