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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This RCA runs on the elected master only and it subscribes all high heap RCA from data nodes
 * within the entire cluster. This can help to reduce the network bandwidth/workload on master node
 * and push computation related workload on data node itself. The RCA uses a cache to keep track of
 * the last three metrics from each node and will mark the node as unhealthy if the last three
 * consecutive flowunits are unhealthy. And if any node is unthleath, the entire cluster will be
 * considered as unhealthy and send out corresponding flowunits to downstream nodes.
 */
public class HighHeapUsageClusterRca extends Rca<ResourceFlowUnit> {

  public static final String HIGH_HEAP_USAGE_CLUSTER_RCA_TABLE = HighHeapUsageClusterRca.class.getSimpleName();
  private static final Logger LOG = LogManager.getLogger(HighHeapUsageClusterRca.class);
  private static final int UNHEALTHY_FLOWUNIT_THRESHOLD = 3;
  private static final int CACHE_EXPIRATION_TIMEOUT = 10;
  private final Rca<ResourceFlowUnit> hotNodeRca;
  private final LoadingCache<String, ImmutableList<ResourceFlowUnit>> nodeStateCache;
  private final int rcaPeriod;
  private int counter;

  public <R extends Rca> HighHeapUsageClusterRca(final int rcaPeriod, final R hotNodeRca) {
    super(5);
    this.hotNodeRca = hotNodeRca;
    this.rcaPeriod = rcaPeriod;
    this.counter = 0;
    nodeStateCache =
        CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(CACHE_EXPIRATION_TIMEOUT, TimeUnit.MINUTES)
                    .build(
                        new CacheLoader<String, ImmutableList<ResourceFlowUnit>>() {
                          public ImmutableList<ResourceFlowUnit> load(String key) {
                            return ImmutableList.copyOf(new ArrayList<>());
                          }
                        });
  }

  private List<GenericSummary> getUnhealthyNodeList() {
    List<GenericSummary> unhealthyNodeList = new ArrayList<>();
    ConcurrentMap<String, ImmutableList<ResourceFlowUnit>> currentMap =
        this.nodeStateCache.asMap();
    for (ClusterDetailsEventProcessor.NodeDetails nodeDetails : ClusterDetailsEventProcessor
        .getDataNodesDetails()) {
      ImmutableList<ResourceFlowUnit> nodeStateList = currentMap.get(nodeDetails.getId());
      if (nodeStateList != null) {
        int unhealthyNodeCnt = 0;
        for (ResourceFlowUnit flowUnit : nodeStateList) {
          if (flowUnit.getResourceContext().getState() == Resources.State.UNHEALTHY) {
            unhealthyNodeCnt++;
          }
        }
        if (unhealthyNodeCnt >= UNHEALTHY_FLOWUNIT_THRESHOLD) {
          unhealthyNodeList.add(nodeStateList.get(0).getResourceSummary());
        }
      }
    }
    return unhealthyNodeList;
  }

  private void readComputeWrite(String nodeId, ResourceFlowUnit flowUnit)
      throws ExecutionException {
    ArrayDeque<ResourceFlowUnit> nodeStateDeque =
        new ArrayDeque<>(this.nodeStateCache.get(nodeId));
    nodeStateDeque.addFirst(flowUnit);
    if (nodeStateDeque.size() > UNHEALTHY_FLOWUNIT_THRESHOLD) {
      nodeStateDeque.removeLast();
    }
    this.nodeStateCache.put(nodeId, ImmutableList.copyOf(nodeStateDeque));
  }

  @Override
  public ResourceFlowUnit operate() {
    List<ResourceFlowUnit> hotNodeRcaFlowUnits = hotNodeRca.getFlowUnits();
    counter += 1;
    for (ResourceFlowUnit hotNodeRcaFlowUnit : hotNodeRcaFlowUnits) {
      if (hotNodeRcaFlowUnit.isEmpty()) {
        continue;
      }
      if (hotNodeRcaFlowUnit.getResourceSummary() instanceof HotNodeSummary) {
        String nodeId = ((HotNodeSummary) hotNodeRcaFlowUnit.getResourceSummary()).getNodeID();
        try {
          readComputeWrite(nodeId, hotNodeRcaFlowUnit);
        } catch (ExecutionException e) {
          LOG.debug("ExecutionException occurs when retrieving key {}", nodeId);
        }
      } else {
        LOG.error("Receive flowunit from unexpected rca node");
      }
    }
    if (counter == rcaPeriod) {
      List<GenericSummary> unhealthyNodeList = getUnhealthyNodeList();
      counter = 0;
      ResourceContext context = null;
      HotClusterSummary summary = null;
      LOG.debug("Unhealthy node id list : {}", unhealthyNodeList);
      if (unhealthyNodeList.size() > 0) {
        context = new ResourceContext(Resources.State.UNHEALTHY);
        summary = new HotClusterSummary(ClusterDetailsEventProcessor.getNodesDetails().size(),
            unhealthyNodeList.size());
        summary.addNestedSummaryList(unhealthyNodeList);
      } else {
        context = new ResourceContext(Resources.State.HEALTHY);
      }
      return new ResourceFlowUnit(System.currentTimeMillis(), context, summary, true);
    } else {
      // we return an empty FlowUnit RCA for now. Can change to healthy (or previous known RCA state)
      LOG.debug("Empty FlowUnit returned for {}", this.getClass().getName());
      return new ResourceFlowUnit(System.currentTimeMillis());
    }
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    final List<FlowUnitMessage> flowUnitMessages =
        args.getWireHopper().readFromWire(args.getNode());
    List<ResourceFlowUnit> flowUnitList = new ArrayList<>();
    LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
    for (FlowUnitMessage flowUnitMessage : flowUnitMessages) {
      flowUnitList.add(ResourceFlowUnit.buildFlowUnitFromWrapper(flowUnitMessage));
    }
    setFlowUnits(flowUnitList);
  }
}
