/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An intermediate buffer that holds flow units received for different vertices from across the
 * cluster.
 */
public class ReceivedFlowUnitStore {

  private static final Logger LOG = LogManager.getLogger(ReceivedFlowUnitStore.class);

  /**
   * Map of vertex to a queue of flow units received for that vertex.
   */
  private ConcurrentMap<String, BlockingQueue<FlowUnitMessage>> flowUnitMap =
      new ConcurrentHashMap<>();

  /**
   * The per vertex flow unit queue size.
   */
  private final int perNodeFlowUnitQSize;

  public ReceivedFlowUnitStore() {
    this(RcaConsts.DEFAULT_PER_NODE_FLOWUNIT_Q_SIZE);
  }

  public ReceivedFlowUnitStore(final int perNodeFlowUnitQSize) {
    this.perNodeFlowUnitQSize = perNodeFlowUnitQSize;
  }

  /**
   * Adds the received flow unit from the network to a dedicated queue for holding flow units for
   * this particular vertex. This queue is then consumed by the wirehopper when the time comes to
   * execute the vertex.
   *
   * @param graphNode       The vertex for which we need to store the remote flow units for.
   * @param flowUnitMessage The actual flow unit message protobuf object that we received from the
   *                        network that needs to be stored.
   * @return true if the enqueue operation succeeded, false if the queue was full and we have to
   *         drop the flow unit.
   */
  public boolean enqueue(final String graphNode, final FlowUnitMessage flowUnitMessage) {
    flowUnitMap.computeIfAbsent(graphNode, s -> new ArrayBlockingQueue<>(perNodeFlowUnitQSize));
    BlockingQueue<FlowUnitMessage> existingQueue = flowUnitMap.get(graphNode);
    boolean retValue = existingQueue.offer(flowUnitMessage);
    if (!retValue) {
      LOG.warn("Dropped flow unit because per vertex queue is full");
      StatsCollector.instance().logException(StatExceptionCode.RCA_VERTEX_RX_BUFFER_FULL_ERROR);
    }

    return retValue;
  }

  /**
   * Drain the flow units enqueued for the vertex.
   *
   * @param graphNode The vertex whose flow units needed to be drained.
   * @return An immutable list containing the flow units received from the network for the vertex.
   */
  public ImmutableList<FlowUnitMessage> drainNode(final String graphNode) {
    final List<FlowUnitMessage> tempList = new ArrayList<>();
    BlockingQueue<FlowUnitMessage> existing = flowUnitMap.get(graphNode);
    if (existing == null) {
      return ImmutableList.of();
    }

    existing.drainTo(tempList);

    return ImmutableList.copyOf(tempList);
  }

  /**
   * Drains out all the flow units for all nodes.
   */
  public List<FlowUnitMessage> drainAll() {
    List<FlowUnitMessage> drained = new ArrayList<>();
    for (final String graphNode : flowUnitMap.keySet()) {
      ImmutableList<FlowUnitMessage> messages = drainNode(graphNode);
      drained.addAll(messages);
    }
    return drained;
  }
}
