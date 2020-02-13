/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.JvmEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ThresholdMain;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RCAScheduler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.MetricsDBProviderTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import java.nio.file.Paths;
import java.util.Collections;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
@SuppressWarnings("serial")
public class PersistFlowUnitAndSummaryTest {
  Queryable queryable;

  static class DummyYoungGenRca extends Rca<ResourceFlowUnit> {
    public <M extends Metric> DummyYoungGenRca(M metric) {
      super(1);
    }

    @Override
    public ResourceFlowUnit operate() {
      System.out.println("!!!!! ruizhen : scheduled \n");
      ResourceContext context = new ResourceContext(Resources.State.UNHEALTHY);
      HotResourceSummary summary = new HotResourceSummary(
          ResourceType.newBuilder().setJVM(JvmEnum.YOUNG_GEN).build(),
          400, 100, "promotion rate in mb/s", 60);
      return new ResourceFlowUnit(System.currentTimeMillis(), context, summary);
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    }
  }

  static class HotNodeRcaX extends HotNodeRca {
    public <R extends Rca> HotNodeRcaX(final int rcaPeriod, R... hotResourceRcas) {
      super(rcaPeriod, hotResourceRcas);
      this.evaluationIntervalSeconds = 1;
    }
  }

  static class HighHeapUsageClusterRcaX extends HighHeapUsageClusterRca {
    public <R extends Rca> HighHeapUsageClusterRcaX(final int rcaPeriod, final R hotNodeRca) {
      super(rcaPeriod, hotNodeRca);
      this.evaluationIntervalSeconds = 1;
    }
  }

  static class DataNodeGraphTest extends AnalysisGraph {

    @Override
    public void construct() {
      Metric heapUsed = new Heap_Used(5);
      addLeaf(heapUsed);
      Rca<ResourceFlowUnit> dummyYoungGenRca = new DummyYoungGenRca(heapUsed);
      dummyYoungGenRca.addAllUpstreams(Collections.singletonList(heapUsed));
      dummyYoungGenRca.addTag(RcaTagConstants.TAG_LOCUS, RcaTagConstants.LOCUS_DATA_NODE);

      Rca<ResourceFlowUnit> nodeRca = new HotNodeRcaX(1, dummyYoungGenRca);
      nodeRca.addTag(RcaTagConstants.TAG_LOCUS, RcaTagConstants.LOCUS_DATA_NODE);
      nodeRca.addAllUpstreams(Collections.singletonList(dummyYoungGenRca));
    }
  }

  static class MasterNodeGraphTest extends AnalysisGraph {

    @Override
    public void construct() {
      Metric heapUsed = new Heap_Used(5);
      heapUsed.addTag(RcaTagConstants.TAG_LOCUS, RcaTagConstants.LOCUS_MASTER_NODE);
      addLeaf(heapUsed);
      Rca<ResourceFlowUnit> dummyYoungGenRca = new DummyYoungGenRca(heapUsed);
      dummyYoungGenRca.addAllUpstreams(Collections.singletonList(heapUsed));
      dummyYoungGenRca.addTag(RcaTagConstants.TAG_LOCUS, RcaTagConstants.LOCUS_MASTER_NODE);

      Rca<ResourceFlowUnit> nodeRca = new HotNodeRcaX(1, dummyYoungGenRca);
      nodeRca.addTag(RcaTagConstants.TAG_LOCUS, RcaTagConstants.LOCUS_MASTER_NODE);
      nodeRca.addAllUpstreams(Collections.singletonList(dummyYoungGenRca));

      Rca<ResourceFlowUnit> highHeapUsageClusterRca =
          new HighHeapUsageClusterRcaX(1, nodeRca);
      highHeapUsageClusterRca.addTag(RcaTagConstants.TAG_LOCUS, RcaTagConstants.LOCUS_MASTER_NODE);
      highHeapUsageClusterRca.addAllUpstreams(Collections.singletonList(nodeRca));
    }
  }

  @Before
  public void before() throws Exception {
    queryable = new MetricsDBProviderTestHelper(false);
  }

  private RCAScheduler startScheduler(RcaConf rcaConf, AnalysisGraph graph, Persistable persistable, Queryable queryable, NodeRole role) {
    RCAScheduler scheduler =
        new RCAScheduler(
            RcaUtil.getAnalysisGraphComponents(graph),
            queryable,
            rcaConf,
            new ThresholdMain(
                Paths.get(RcaConsts.TEST_CONFIG_PATH, "thresholds").toString(), rcaConf),
            persistable,
            new WireHopper(null, null, null, null, null));
    scheduler.setRole(role);
    scheduler.start();
    return scheduler;
  }

  @Test
  public void testPersistSummaryOnDataNode() throws Exception {
    try {
      ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
      clusterDetailsEventProcessorTestHelper.addNodeDetails("node1", "127.0.0.0", false);
      clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
    } catch (Exception e) {
      Assert.assertTrue("got exception when generating cluster details event", false);
      return;
    }
    AnalysisGraph graph = new DataNodeGraphTest();
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());
    Persistable persistable = PersistenceFactory.create(rcaConf);
    RCAScheduler scheduler = startScheduler(rcaConf, graph, persistable, this.queryable, AllMetrics.NodeRole.DATA);
    Thread.sleep(4000);
    String readTableStr = persistable.read();
    Assert.assertTrue(readTableStr.contains("HotResourceSummary"));
    Assert.assertTrue(readTableStr.contains("DummyYoungGenRca"));
    Assert.assertTrue(readTableStr.contains("HotNodeSummary"));
    Assert.assertTrue(readTableStr.contains("HotNodeRcaX"));
    Assert.assertFalse(readTableStr.contains("HighHeapUsageClusterRcaX"));
    scheduler.shutdown();
    persistable.close();
  }

  @Test
  public void testPersistSummaryOnMasterNode() throws Exception {
    try {
      ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
      clusterDetailsEventProcessorTestHelper.addNodeDetails("node1", "127.0.0.0", true);
      clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
    } catch (Exception e) {
      Assert.assertTrue("got exception when generating cluster details event", false);
      return;
    }
    AnalysisGraph graph = new MasterNodeGraphTest();
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca_elected_master.conf").toString());
    Persistable persistable = PersistenceFactory.create(rcaConf);
    RCAScheduler scheduler = startScheduler(rcaConf, graph, persistable, this.queryable, NodeRole.ELECTED_MASTER);
    Thread.sleep(4000);
    String readTableStr = persistable.read();
    Assert.assertTrue(readTableStr.contains("HotResourceSummary"));
    Assert.assertTrue(readTableStr.contains("DummyYoungGenRca"));
    Assert.assertTrue(readTableStr.contains("HotNodeSummary"));
    Assert.assertTrue(readTableStr.contains("HotNodeRcaX"));
    Assert.assertTrue(readTableStr.contains("HighHeapUsageClusterRcaX"));
    scheduler.shutdown();
    persistable.close();
  }

  @After
  public void cleanup() throws Exception {
    if (queryable != null) {
      queryable.getMetricsDB().close();
      queryable.getMetricsDB().deleteOnDiskFile();
    }
  }
}
