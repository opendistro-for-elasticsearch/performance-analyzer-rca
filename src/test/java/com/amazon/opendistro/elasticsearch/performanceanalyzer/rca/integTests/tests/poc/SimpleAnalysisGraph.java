/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.poc;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.INDEX_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.SHARD_ID;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotShardSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard.IndexShardKey;
import java.util.ArrayList;
import java.util.List;
import org.jooq.Record;

public class SimpleAnalysisGraph extends ElasticSearchAnalysisGraph {
  public static class NodeRca extends Rca<ResourceFlowUnit<HotNodeSummary>> {
    private final CPU_Utilization cpuUtilization;

    public NodeRca(CPU_Utilization cpu_utilization) {
      super(1);
      this.cpuUtilization = cpu_utilization;
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
      final List<FlowUnitMessage> flowUnitMessages =
          args.getWireHopper().readFromWire(args.getNode());
      List<ResourceFlowUnit<HotNodeSummary>> flowUnitList = new ArrayList<>();
      for (FlowUnitMessage flowUnitMessage : flowUnitMessages) {
        flowUnitList.add(ResourceFlowUnit.buildFlowUnitFromWrapper(flowUnitMessage));
      }
      setFlowUnits(flowUnitList);
    }

    @Override
    public ResourceFlowUnit<HotNodeSummary> operate() {
      double maxCpu = 0;
      IndexShardKey indexShardKey = null;
      for (MetricFlowUnit metricFlowUnit : cpuUtilization.getFlowUnits()) {
        if (metricFlowUnit.getData() != null) {
          // Go through all the entries and find out the shard with the highest CPU
          // utilization.
          for (Record record : metricFlowUnit.getData()) {
            try {
              String indexName = record.getValue(INDEX_NAME.toString(), String.class);
              // System.out.println(record);
              Integer shardId = record.getValue(SHARD_ID.toString(), Integer.class);
              if (indexName != null && shardId != null) {
                double usage = record.getValue(MetricsDB.MAX, Double.class);
                if (usage > maxCpu) {
                  maxCpu = usage;
                  indexShardKey = IndexShardKey.buildIndexShardKey(record);
                }
              }
            } catch (IllegalArgumentException ex) {

            }
          }
        }
      }
      InstanceDetails instanceDetails = getInstanceDetails();
      HotNodeSummary nodeSummary = new HotNodeSummary(instanceDetails.getInstanceId(),
          instanceDetails.getInstanceIp());
      ResourceFlowUnit rfu;
      if (indexShardKey != null) {
        //System.out.println("NodeRca running on " + instanceDetails.getInstanceId());

        HotShardSummary summary = new HotShardSummary(
            indexShardKey.getIndexName(),
            String.valueOf(indexShardKey.getShardId()),
            instanceDetails.getInstanceId().toString(),
            0);
        summary.setcpuUtilization(maxCpu);
        nodeSummary.appendNestedSummary(summary);
        rfu = new ResourceFlowUnit<>(
            System.currentTimeMillis(),
            new ResourceContext(Resources.State.UNHEALTHY),
            nodeSummary,
            true);

        //System.out.println("NODE RCA: " + rfu);
      } else {
        rfu = new ResourceFlowUnit<>(System.currentTimeMillis());
      }
      return rfu;
    }
  }

  public static class ClusterRca extends Rca<ResourceFlowUnit<HotClusterSummary>> {
    private final NodeRca nodeRca;

    public ClusterRca(NodeRca nodeRca) {
      super(1);
      this.nodeRca = nodeRca;
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
      throw new IllegalArgumentException(name() + "'s generateFlowUnitListFromWire() should not "
          + "be required.");
    }

    // The cluster level RCA goes through all the nodeLevel summaries and then picks the node
    // with the highest CPU and states which shard it is the highest for.
    @Override
    public ResourceFlowUnit<HotClusterSummary> operate() {
      final List<ResourceFlowUnit<HotNodeSummary>> resourceFlowUnits = nodeRca.getFlowUnits();
      HotClusterSummary summary = new HotClusterSummary(
          getAllClusterInstances().size(), 1);

      final InstanceDetails.Id defaultId = new InstanceDetails.Id("default-id");
      final InstanceDetails.Ip defaultIp = new InstanceDetails.Ip("1.1.1.1");

      InstanceDetails.Id hotNodeId = defaultId;
      InstanceDetails.Ip hotsNodeAddr = defaultIp;
      String hotShard = "";
      String hotShardIndex = "";
      double cpuUtilization = 0.0;

      for (final ResourceFlowUnit<HotNodeSummary> resourceFlowUnit : resourceFlowUnits) {
        if (resourceFlowUnit.isEmpty()) {
          continue;
        }
        HotNodeSummary nodeSummary = resourceFlowUnit.getSummary();
        HotShardSummary hotShardSummary = nodeSummary.getHotShardSummaryList().get(0);
        double cpu = hotShardSummary.getCpuUtilization();
        if (cpu > cpuUtilization) {
          hotNodeId = nodeSummary.getNodeID();
          hotsNodeAddr = nodeSummary.getHostAddress();
          hotShard = hotShardSummary.getShardId();
          hotShardIndex = hotShardSummary.getIndexName();
          cpuUtilization = cpu;
        }
      }

      ResourceFlowUnit<HotClusterSummary> rfu;
      if (!hotNodeId.equals(defaultId)) {
        HotClusterSummary hotClusterSummary = new HotClusterSummary(
            getAllClusterInstances().size(), 1);
        HotNodeSummary hotNodeSummary = new HotNodeSummary(hotNodeId, hotsNodeAddr);
        HotShardSummary hotShardSummary =
            new HotShardSummary(hotShardIndex, hotShard, hotNodeId.toString(), 0);
        hotShardSummary.setcpuUtilization(cpuUtilization);
        hotNodeSummary.appendNestedSummary(hotShardSummary);
        hotClusterSummary.appendNestedSummary(hotNodeSummary);

        rfu = new ResourceFlowUnit<>(
            System.currentTimeMillis(),
            new ResourceContext(Resources.State.UNHEALTHY),
            hotClusterSummary, true);
      } else {
        rfu = new ResourceFlowUnit<>(System.currentTimeMillis());
      }
      //System.out.println("CLUSTER RCA: " + rfu);
      return rfu;
    }
  }
}
