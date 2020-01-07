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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.flowunit.HighHeapUsageClusterFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.flowunit.HighHeapUsageFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
public class HighHeapUsageClusterRca extends Rca<HighHeapUsageClusterFlowUnit> {

  private static final Logger LOG = LogManager.getLogger(HighHeapUsageOldGenRca.class);
  private static final int RCA_PERIOD = 12;
  private static final int UNHEALTHY_FLOWUNIT_THRESHOLD = 3;
  private static final int CACHE_EXPIRATION_TIMEOUT = 10;
  protected int counter;
  private final Rca<HighHeapUsageFlowUnit> highHeapUsageRca;
  private final LoadingCache<String, ImmutableList<ResourceContext.State>> nodeStateCache;

  public <R extends Rca<HighHeapUsageFlowUnit>> HighHeapUsageClusterRca(
      long evaluationIntervalSeconds, final R highHeapUsageRca) {
    super(evaluationIntervalSeconds);
    this.highHeapUsageRca = highHeapUsageRca;
    counter = 0;
    nodeStateCache =
        CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(CACHE_EXPIRATION_TIMEOUT, TimeUnit.MINUTES)
                    .build(
                        new CacheLoader<String, ImmutableList<ResourceContext.State>>() {
                          public ImmutableList<ResourceContext.State> load(String key) {
                            return ImmutableList.copyOf(new ArrayList<>());
                          }
                        });
  }

  private List<String> getUnhealthyNodeList() {
    List<String> unhealthyNodeList = new ArrayList<>();
    ConcurrentMap<String, ImmutableList<ResourceContext.State>> currentMap =
        this.nodeStateCache.asMap();
    for (ClusterDetailsEventProcessor.NodeDetails nodeDetails : ClusterDetailsEventProcessor
        .getDataNodesDetails()) {
      ImmutableList<ResourceContext.State> nodeStateList = currentMap.get(nodeDetails.getId());
      if (nodeStateList == null) {
        unhealthyNodeList.add(nodeDetails.getId());
      } else {
        int unhealthyNodeCnt = 0;
        for (ResourceContext.State state : nodeStateList) {
          if (state == ResourceContext.State.UNHEALTHY) {
            unhealthyNodeCnt++;
          }
        }
        if (unhealthyNodeCnt >= UNHEALTHY_FLOWUNIT_THRESHOLD) {
          unhealthyNodeList.add(nodeDetails.getId());
        }
      }
    }
    return unhealthyNodeList;
  }

  private synchronized void readComputeWrite(String nodeId, ResourceContext.State state)
      throws ExecutionException {
    ArrayDeque<ResourceContext.State> nodeStateDeque =
        new ArrayDeque<>(this.nodeStateCache.get(nodeId));
    nodeStateDeque.addFirst(state);
    if (nodeStateDeque.size() > UNHEALTHY_FLOWUNIT_THRESHOLD) {
      nodeStateDeque.removeLast();
    }
    this.nodeStateCache.put(nodeId, ImmutableList.copyOf(nodeStateDeque));
  }

  @Override
  public HighHeapUsageClusterFlowUnit operate() {
    List<HighHeapUsageFlowUnit> highHeapUsageRcaFlowUnits = highHeapUsageRca.getFlowUnits();
    counter += 1;
    for (ResourceFlowUnit highHeapUsageRcaFlowUnit : highHeapUsageRcaFlowUnits) {
      // TODO: flowunit.isEmpty() is set only when the flowunit is empty. unknown state should be
      // allowed
      if (highHeapUsageRcaFlowUnit.isEmpty()
          || highHeapUsageRcaFlowUnit.getResourceContext().isUnknown()) {
        continue;
      }
      List<List<String>> highHeapUsageRcaData = highHeapUsageRcaFlowUnit.getData();
      if (!highHeapUsageRcaFlowUnits.isEmpty() && !highHeapUsageRcaData.isEmpty()) {
        // TODO: List<List<>> needs to be changed
        String nodeId = highHeapUsageRcaData.get(1).get(0);
        try {
          readComputeWrite(nodeId, highHeapUsageRcaFlowUnit.getResourceContext().getState());
        } catch (ExecutionException e) {
          LOG.debug("ExecutionException occurs when retrieving key {}", nodeId);
        }
      }
    }
    if (counter == RCA_PERIOD) {
      List<List<String>> ret = new ArrayList<>();
      List<String> unhealthyNodeList = getUnhealthyNodeList();
      counter = 0;
      LOG.debug("Unhealthy node id list : {}", unhealthyNodeList);
      if (unhealthyNodeList.size() > 0) {
        String row = unhealthyNodeList.stream().collect(Collectors.joining(" "));
        ret.addAll(
            Arrays.asList(
                Collections.singletonList("Unhealthy node(s)"), Collections.singletonList(row)));
        return new HighHeapUsageClusterFlowUnit(
            System.currentTimeMillis(),
            ret,
            new ResourceContext(ResourceContext.Resource.HEAP, ResourceContext.State.UNHEALTHY));
      } else {
        ret.addAll(
            Arrays.asList(
                Collections.singletonList("Unhealthy node(s)"),
                Collections.singletonList("All nodes are healthy")));
        return new HighHeapUsageClusterFlowUnit(
            System.currentTimeMillis(),
            ret,
            new ResourceContext(ResourceContext.Resource.HEAP, ResourceContext.State.HEALTHY));
      }
    } else {
      // we return an empty FlowUnit RCA for now. Can change to healthy (or previous known RCA
      // state)
      LOG.debug("Empty FlowUnit returned for {}", this.getClass().getName());
      return new HighHeapUsageClusterFlowUnit(System.currentTimeMillis(),
          ResourceContext.generic());
    }
  }

  /**
   * TODO: Move this method out of the RCA class. The scheduler should set the flow units it drains
   * from the Rx queue between the scheduler and the networking thread into the node.
   *
   * @param args The wrapper around the flow unit operation.
   */
  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    final List<FlowUnitWrapper> flowUnitWrappers =
        args.getWireHopper().readFromWire(args.getNode());
    List<HighHeapUsageClusterFlowUnit> flowUnitList = new ArrayList<>();
    LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
    for (FlowUnitWrapper messageWrapper : flowUnitWrappers) {
      flowUnitList.add(HighHeapUsageClusterFlowUnit.buildFlowUnitFromWrapper(messageWrapper));
    }

    setFlowUnits(flowUnitList);
  }
}
