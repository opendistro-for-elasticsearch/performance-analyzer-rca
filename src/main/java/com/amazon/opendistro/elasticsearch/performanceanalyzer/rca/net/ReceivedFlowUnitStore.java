package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.FlowUnit;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ReceivedFlowUnitStore {

  private static final int MAX_Q_SIZE = 200;
  private ConcurrentMap<String, BlockingQueue<FlowUnitMessage>> flowUnitMap =
      new ConcurrentHashMap<>();

  public boolean enqueue(final String graphNode, final FlowUnitMessage flowUnitMessage) {
    BlockingQueue<FlowUnitMessage> existingQueue = flowUnitMap.get(graphNode);
    if (existingQueue == null) {
      existingQueue = new ArrayBlockingQueue<>(MAX_Q_SIZE);
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
