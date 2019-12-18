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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("unchecked")
public class NetPersistor {
  private static final Logger LOG = LogManager.getLogger(NetPersistor.class);
  private static final int PER_GRAPH_NODE_FLOW_UNIT_QUEUE_CAPACITY = 200;
  ConcurrentMap<String, BlockingQueue<FlowUnitWrapper>> graphNodeToFlowUnitMap =
      new ConcurrentHashMap<>();

  public List<FlowUnitWrapper> read(final String graphNode) {
    if (graphNodeToFlowUnitMap.containsKey(graphNode)) {
      final BlockingQueue<FlowUnitWrapper> flowUnitQueue = graphNodeToFlowUnitMap.get(graphNode);
      final List<FlowUnitWrapper> returnList = new ArrayList<>();
      flowUnitQueue.drainTo(returnList);
      return returnList;
    }

    return new ArrayList<>();
  }

  public void write(final String graphNode, final FlowUnitMessage flowUnitMessage) {
    graphNodeToFlowUnitMap.putIfAbsent(
        graphNode, new ArrayBlockingQueue<>(PER_GRAPH_NODE_FLOW_UNIT_QUEUE_CAPACITY));
    final BlockingQueue<FlowUnitWrapper> flowUnitQueue = graphNodeToFlowUnitMap.get(graphNode);

    if (!flowUnitQueue.offer(FlowUnitWrapper.buildFlowUnitWrapperFromMessage(flowUnitMessage))) {
      LOG.debug("Failed to add flow unit to the buffer. Dropping the flow unit.");
    }
  }
}
