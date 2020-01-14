package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Networking component that handles receiving of flow units.
 */
public class Receiver {

  private static final Logger LOG = LogManager.getLogger(Receiver.class);
  private final NetworkRequestQueue<FlowUnitMessage> rxQ;
  private final ScheduledExecutorService threadPool;
  private final ReceivedFlowUnitStore receivedFlowUnitStore;
  private final ReceiveTask recvTask;

  public Receiver(
      final NetworkRequestQueue<FlowUnitMessage> rxQ,
      final ScheduledExecutorService threadPool,
      final ReceivedFlowUnitStore receivedFlowUnitStore,
      final ReceiveTask recvTask) {
    this.rxQ = rxQ;
    this.threadPool = threadPool;
    this.receivedFlowUnitStore = receivedFlowUnitStore;
    this.recvTask = recvTask;
  }

  public void start() {
    threadPool.scheduleAtFixedRate(recvTask, 0, 250, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    // drain out the queue to stop processing.
    rxQ.drain();
  }

  public boolean enqueue(final FlowUnitMessage flowUnitMessage) {
    return rxQ.offer(flowUnitMessage);
  }

  public synchronized ImmutableList<FlowUnitMessage> getFlowUnitsForNode(final String graphNode) {
    return receivedFlowUnitStore.drainNode(graphNode);
  }
}
