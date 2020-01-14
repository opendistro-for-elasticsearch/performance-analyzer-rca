package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReceiveTask implements Runnable {

  private static final Logger LOG = LogManager.getLogger(ReceiveTask.class);
  private final NetworkRequestQueue<FlowUnitMessage> rxQ;
  private final ReceivedFlowUnitStore receivedFlowUnitStore;
  private final NodeStateManager nodeStateManager;

  public ReceiveTask(
      NetworkRequestQueue<FlowUnitMessage> rxQ,
      ReceivedFlowUnitStore receivedFlowUnitStore,
      NodeStateManager nodeStateManager) {
    this.rxQ = rxQ;
    this.receivedFlowUnitStore = receivedFlowUnitStore;
    this.nodeStateManager = nodeStateManager;
  }

  /**
   * When an object implementing interface <code>Runnable</code> is used to create a thread,
   * starting the thread causes the object's
   * <code>run</code> method to be called in that separately executing
   * thread.
   * The general contract of the method <code>run</code> is that it may take any action whatsoever.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    for (final FlowUnitMessage flowUnitMessage : rxQ.drain()) {
      String host = flowUnitMessage.getEsNode();
      String graphNode = flowUnitMessage.getGraphNode();

      nodeStateManager.updateReceiveTime(host, graphNode, System.currentTimeMillis());
      if (!receivedFlowUnitStore.enqueue(graphNode, flowUnitMessage)) {
        LOG.warn("Dropped a flowunit because buffer was full for vertex: {}", graphNode);
        StatsCollector.instance().logException(StatExceptionCode.RCA_BUFFER_FULL_ERROR);
      }
    }
  }
}
