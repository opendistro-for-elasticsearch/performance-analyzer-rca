package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.QueueCapacity;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ThreadPoolEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;

import java.util.ArrayList;
import java.util.List;

// This is a sample decider implementation to finalize decision maker interfaces.
// TODO: 1. Read action priorities from a configurable yml
// TODO: 2. Read current queue capacity from NodeConfigurationRca (PR #252)

public class QueueHealthDecider extends Decider {

  public static String NAME = "queue_health";

  private QueueRejectionClusterRca queueRejectionRca;
  List<String> actionsByUserPriority = new ArrayList<>();
  private int counter = 0;

  public QueueHealthDecider(long evalIntervalSeconds, int decisionFrequency, QueueRejectionClusterRca queueRejectionClusterRca) {
    // TODO: Also consume NodeConfigurationRca
    super(evalIntervalSeconds, decisionFrequency);
    this.queueRejectionRca = queueRejectionClusterRca;
    configureActionPriority();
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Decision operate() {
    Decision decision = new Decision(System.currentTimeMillis(), NAME);
    counter += 1;
    if (counter < decisionFrequency) {
      return decision;
    }

    counter = 0;
    if (queueRejectionRca.getFlowUnits().isEmpty()) {
      return decision;
    }

    HotClusterSummary clusterSummary = queueRejectionRca.getFlowUnits().get(0).getSummary();
    for (HotNodeSummary nodeSummary : clusterSummary.getHotNodeSummaryList()) {
      NodeKey esNode = new NodeKey(nodeSummary.getNodeID(), nodeSummary.getHostAddress());
      for (HotResourceSummary resource : nodeSummary.getHotResourceSummaryList()) {
        decision.addAction(computeBestAction(esNode, resource.getResourceType().getThreadPool()));
      }
    }

    return decision;
  }

  private void configureActionPriority() {
    // TODO: Input from user configured yml
    this.actionsByUserPriority.add(QueueCapacity.NAME);
  }

  /**
   * Evaluate the most relevant action for a node
   *
   * <p>Action relevance decided based on user configured priorities for now, this can be modified
   * to consume better signals going forward.
   */
  private Action computeBestAction(NodeKey esNode, ThreadPoolEnum threadPool) {
    Action action = null;
    for (String actionName : actionsByUserPriority) {
      action =
          getAction(actionName, esNode, threadPool, getNodeQueueCapacity(esNode, threadPool), true);
      if (action != null) {
        break;
      }
    }
    return action;
  }

  private Action getAction(String actionName, NodeKey esNode, ThreadPoolEnum threadPool, int currCapacity, boolean increase) {
    switch (actionName) {
      case QueueCapacity.NAME:
        return configureQueueCapacity(esNode, threadPool, currCapacity, increase);
      default:
        return null;
    }
  }

  private QueueCapacity configureQueueCapacity(NodeKey esNode, ThreadPoolEnum threadPool, int currentCapacity, boolean increase) {
    QueueCapacity action = new QueueCapacity(esNode, threadPool, currentCapacity, increase);
    if (action.isActionable()) {
      return action;
    }
    return null;
  }

  private int getNodeQueueCapacity(NodeKey esNode, ThreadPoolEnum threadPool) {
    // TODO: use NodeConfigurationRca to return capacity, for now returning defaults
    if (threadPool.equals(ThreadPoolEnum.SEARCH_QUEUE)) {
      return 1000;
    }
    return 100;
  }
}
