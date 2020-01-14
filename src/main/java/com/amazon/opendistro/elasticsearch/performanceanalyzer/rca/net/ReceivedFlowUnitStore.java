package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ReceivedFlowUnitStore {

  private static final int DEFAULT_PER_NODE_FLOWUNIT_Q_SIZE = 200;
  private ConcurrentMap<String, BlockingQueue<FlowUnitMessage>> flowUnitMap =
      new ConcurrentHashMap<>();
  private final int perNodeFlowUnitQSize;

  public ReceivedFlowUnitStore() {
    this(DEFAULT_PER_NODE_FLOWUNIT_Q_SIZE);
  }

  public ReceivedFlowUnitStore(final int perNodeFlowUnitQSize) {
    this.perNodeFlowUnitQSize = perNodeFlowUnitQSize;
  }

  public boolean enqueue(final String graphNode, final FlowUnitMessage flowUnitMessage) {
    BlockingQueue<FlowUnitMessage> existingQueue = flowUnitMap.get(graphNode);
    if (existingQueue == null) {
      existingQueue = new ArrayBlockingQueue<>(perNodeFlowUnitQSize);
    }

    boolean retValue = existingQueue.offer(flowUnitMessage);
    flowUnitMap.put(graphNode, existingQueue);

    return retValue;
  }

  public ImmutableList<FlowUnitMessage> drainNode(final String graphNode) {
    final List<FlowUnitMessage> tempList = new ArrayList<>();
    BlockingQueue<FlowUnitMessage> existing = flowUnitMap.get(graphNode);
    if (existing == null) {
      return ImmutableList.of();
    }

    existing.drainTo(tempList);

    return ImmutableList.copyOf(tempList);
  }
}
