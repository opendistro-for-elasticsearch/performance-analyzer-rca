package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.MalformedAnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class NonLeafNode extends Node implements Operable {
  private static final Logger LOG = LogManager.getLogger(NonLeafNode.class);

  public NonLeafNode(int level, long evaluationDurationSeconds) {
    super(level, evaluationDurationSeconds);
    Stats stats = Stats.getInstance();
    stats.incrementTotalNodesCount();
  }

  /**
   * Add all the upstream nodes of this node in one shot. This method cannot be called on a same
   * node twice. This restriction is important to ensure the absence of cycles.
   *
   * @param upstreams The list of all the upstream nodes.
   */
  public void addAllUpstreams(List<Node> upstreams) {
    int minGraphId = validateAndAddDownstream(upstreams);
    setGraphId(updateGraphs(minGraphId, upstreams));
    this.upStreams = upstreams;
  }

  /**
   * TODO: Update
   *
   * @param minId the current minimum id of the graph.
   * @param upstreams The upstream vertices to update the graph with.
   * @return The new minimum id of the graph post update.
   */
  private int updateGraphs(int minId, List<Node> upstreams) {
    final Queue<Node> bfsQueue = new LinkedList<>(upstreams);
    while (!bfsQueue.isEmpty()) {
      final Node currentNode = bfsQueue.poll();
      int graphId = currentNode.getGraphId();
      if (minId != graphId) {
        currentNode.setGraphId(minId);
        Stats.getInstance().removeGraph(graphId);
      }
      final List<Node> currentNodeUpstreams = currentNode.getUpstreams();
      bfsQueue.addAll(currentNodeUpstreams);
    }
    return minId;
  }

  /**
   * Adding the upstream nodes to the current node requires some validations. 1. The call to the
   * allAllUpstreams should be made only once. 2. The evaluation interval of the node is greater
   * than or equal to all the nodes it depends on. 3. The Metrics this node depends on should
   * already be added to the FlowField.
   */
  private int validateAndAddDownstream(List<Node> upstreams) {
    if (this.upStreams != null) {
      throw new MalformedAnalysisGraph("All upstreams of a node should be added at once.");
    }

    StringBuilder metricNodesNotAdded = new StringBuilder();
    String metricNodeDelimeter = "";
    boolean foundNotAddedMetrics = false;
    int maxLevel = 0;
    int minId = Integer.MAX_VALUE;

    for (Node node : upstreams) {
      if (node instanceof Metric && !((Metric) node).isAddedToFlowField()) {
        metricNodesNotAdded.append(metricNodeDelimeter).append(node.getClass().getSimpleName());
        metricNodeDelimeter = ", ";
        foundNotAddedMetrics = true;
      }

      // When upstreams are added to a node, this also leads to the possibility of merging two
      // graphs if they
      // are not already. If so, we want to use the min of the two IDs as the ID of the merged
      // graph.
      minId = Integer.min(minId, node.getGraphId());

      int nodeLevel = node.getLevel();
      // The level of the current node should be one higher than all its dependents.
      maxLevel = nodeLevel > maxLevel ? nodeLevel : maxLevel;

      // Every node also tracks its downstream nodes.
      node.addDownstream(this);
    }

    if (foundNotAddedMetrics) {
      throw new MalformedAnalysisGraph(
          String.format(
              "These metrics are not added to the AnalysisGraph yet: %s ",
              metricNodesNotAdded.toString()));
    }

    setLevel(maxLevel + 1);
    return minId;
  }
}
