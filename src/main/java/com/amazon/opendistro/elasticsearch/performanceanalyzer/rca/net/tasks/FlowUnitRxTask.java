package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.ReceivedFlowUnitStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlowUnitRxTask implements Runnable {

  private static final Logger LOG = LogManager.getLogger(FlowUnitRxTask.class);
  private final NodeStateManager nodeStateManager;
  private final ReceivedFlowUnitStore receivedFlowUnitStore;
  private final FlowUnitMessage flowUnitMessage;

  public FlowUnitRxTask(
      final NodeStateManager nodeStateManager,
      final ReceivedFlowUnitStore receivedFlowUnitStore,
      final FlowUnitMessage flowUnitMessage) {
    this.nodeStateManager = nodeStateManager;
    this.receivedFlowUnitStore = receivedFlowUnitStore;
    this.flowUnitMessage = flowUnitMessage;
  }

  /**
   * Updates the per vertex flow unit collection.
   * @see Thread#run()
   */
  @Override
  public void run() {
    final String host = flowUnitMessage.getEsNode();
    final String vertex = flowUnitMessage.getGraphNode();

    nodeStateManager.updateReceiveTime(host, vertex, System.currentTimeMillis());
    if (!receivedFlowUnitStore.enqueue(vertex, flowUnitMessage)) {
      LOG.warn("Dropped a flow unit because the vertex buffer was full for vertex: {}", vertex);
      StatsCollector.instance().logMetric(RcaConsts.VERTEX_BUFFER_FULL_METRIC);
    }
  }
}
