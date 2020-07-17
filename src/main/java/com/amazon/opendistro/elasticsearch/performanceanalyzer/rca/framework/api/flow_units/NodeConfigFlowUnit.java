/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage.SummaryOneofCase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.HotNodeSummaryMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.HashMap;

/**
 * a flowunit type to carry ES node configurations (queue/cache capacities, etc.)
 */
public class NodeConfigFlowUnit extends ResourceFlowUnit<HotNodeSummary> {

  private final HashMap<Resource, HotResourceSummary> configMap;

  public NodeConfigFlowUnit(long timeStamp) {
    super(timeStamp);
    this.configMap = new HashMap<>();
  }

  public NodeConfigFlowUnit(long timeStamp, NodeKey nodeKey) {
    super(timeStamp, new ResourceContext(Resources.State.HEALTHY), null, false);
    this.setSummary(new HotNodeSummary(nodeKey.getNodeId(), nodeKey.getHostAddress()));
    this.configMap = new HashMap<>();
  }

  /**
   * Add new config setting to flowunit
   * @param resource config setting type
   * @param value config setting value
   */
  public void addConfig(Resource resource, double value) {
    HotResourceSummary configSummary = new HotResourceSummary(resource, Double.NaN, value, 0);
    configMap.put(resource, configSummary);
  }

  /**
   * Add new config setting to flowunit
   * @param configSummary config setting summary object
   */
  public void addConfig(HotResourceSummary configSummary) {
    configMap.put(configSummary.getResource(), configSummary);
  }

  /**
   * check if the config setting exist in flowunit
   * @param resource config setting type
   * @return if config exist
   */
  public boolean hasConfig(Resource resource) {
    return configMap.containsKey(resource);
  }

  /**
   * read the config value of the config setting from flowunit
   * @param resource config setting type
   * @return config setting value
   */
  public double readConfig(Resource resource) {
    HotResourceSummary configSummary = configMap.getOrDefault(resource, null);
    if (configSummary == null) {
      return Double.NaN;
    }
    return configSummary.getValue();
  }

  @Override
  public boolean isEmpty() {
    return configMap.isEmpty();
  }

  /**
   * build NodeConfigFlowUnit from the protobuf message
   */
  public static NodeConfigFlowUnit buildFlowUnitFromWrapper(final FlowUnitMessage message) {
    NodeConfigFlowUnit nodeConfigFlowUnit;
    if (message.getSummaryOneofCase() == SummaryOneofCase.HOTNODESUMMARY) {
      HotNodeSummaryMessage nodeSummaryMessage = message.getHotNodeSummary();
      NodeKey nodeKey = new NodeKey(nodeSummaryMessage.getNodeID(),
          nodeSummaryMessage.getHostAddress());
      nodeConfigFlowUnit = new NodeConfigFlowUnit(message.getTimeStamp(), nodeKey);
      if (nodeSummaryMessage.hasHotResourceSummaryList()) {
        for (int i = 0;
            i < nodeSummaryMessage.getHotResourceSummaryList().getHotResourceSummaryCount(); i++) {
          nodeConfigFlowUnit.addConfig(
              HotResourceSummary.buildHotResourceSummaryFromMessage(
                  nodeSummaryMessage.getHotResourceSummaryList().getHotResourceSummary(i))
          );
        }
      }
    } else {
      nodeConfigFlowUnit = new NodeConfigFlowUnit(message.getTimeStamp());
    }
    return nodeConfigFlowUnit;
  }
}
