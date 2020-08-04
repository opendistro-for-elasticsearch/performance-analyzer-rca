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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerThreads;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.ThreadProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
@SuppressWarnings("serial")
public class PersistFlowUnitAndSummaryTest {
  Queryable queryable;

  static class DummyYoungGenRca extends Rca<ResourceFlowUnit<HotResourceSummary>> {
    public <M extends Metric> DummyYoungGenRca(M metric) {
      super(1);
    }

    @Override
    public ResourceFlowUnit<HotResourceSummary> operate() {
      ResourceContext context = new ResourceContext(Resources.State.UNHEALTHY);
      HotResourceSummary summary = new HotResourceSummary(
          ResourceUtil.YOUNG_GEN_PROMOTION_RATE,
          400, 100, 60);
      return new ResourceFlowUnit<>(System.currentTimeMillis(), context, summary);
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    }
  }

  static class HotNodeRcaX extends HotNodeRca {
    public <R extends Rca<ResourceFlowUnit<HotResourceSummary>>> HotNodeRcaX(final int rcaPeriod, R... hotResourceRcas) {
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

  static class DataNodeGraph extends AnalysisGraph {

    @Override
    public void construct() {
      Metric heapUsed = new Heap_Used(5);
      addLeaf(heapUsed);
      Rca<ResourceFlowUnit<HotResourceSummary>> dummyYoungGenRca = new DummyYoungGenRca(heapUsed);
      dummyYoungGenRca.addAllUpstreams(Collections.singletonList(heapUsed));
      dummyYoungGenRca.addTag(RcaTagConstants.TAG_LOCUS, RcaTagConstants.LOCUS_DATA_NODE);

      Rca<ResourceFlowUnit<HotNodeSummary>> nodeRca = new HotNodeRcaX(1, dummyYoungGenRca);
      nodeRca.addTag(RcaTagConstants.TAG_LOCUS, RcaTagConstants.LOCUS_DATA_NODE);
      nodeRca.addAllUpstreams(Collections.singletonList(dummyYoungGenRca));
    }
  }

  static class MasterNodeGraph extends AnalysisGraph {

    @Override
    public void construct() {
      Metric heapUsed = new Heap_Used(5);
      heapUsed.addTag(RcaTagConstants.TAG_LOCUS, RcaTagConstants.LOCUS_DATA_MASTER_NODE);
      addLeaf(heapUsed);
      Rca<ResourceFlowUnit<HotResourceSummary>> dummyYoungGenRca = new DummyYoungGenRca(heapUsed);
      dummyYoungGenRca.addAllUpstreams(Collections.singletonList(heapUsed));
      dummyYoungGenRca.addTag(RcaTagConstants.TAG_LOCUS, RcaTagConstants.LOCUS_DATA_MASTER_NODE);

      Rca<ResourceFlowUnit<HotNodeSummary>> nodeRca = new HotNodeRcaX(1, dummyYoungGenRca);
      nodeRca.addTag(RcaTagConstants.TAG_LOCUS, RcaTagConstants.LOCUS_DATA_MASTER_NODE);
      nodeRca.addAllUpstreams(Collections.singletonList(dummyYoungGenRca));

      HighHeapUsageClusterRca highHeapUsageClusterRca =
          new HighHeapUsageClusterRcaX(1, nodeRca);
      highHeapUsageClusterRca.addTag(RcaTagConstants.TAG_LOCUS, RcaTagConstants.LOCUS_MASTER_NODE);
      highHeapUsageClusterRca.addAllUpstreams(Collections.singletonList(nodeRca));
    }
  }

  @Before
  public void before() throws Exception {
    queryable = new MetricsDBProviderTestHelper(false);
  }

  private RCAScheduler startScheduler(RcaConf rcaConf, AnalysisGraph graph, Persistable persistable,
                                      Queryable queryable, AppContext appContext) {
    RCAScheduler scheduler =
        new RCAScheduler(
            RcaUtil.getAnalysisGraphComponents(graph),
            queryable,
            rcaConf,
            new ThresholdMain(
                Paths.get(RcaConsts.TEST_CONFIG_PATH, "thresholds").toString(), rcaConf),
            persistable,
            new WireHopper(null, null, null, null,
                null, appContext),
            appContext
            );
    ThreadProvider threadProvider = new ThreadProvider();
    Thread rcaSchedulerThread =
        threadProvider.createThreadForRunnable(scheduler::start, PerformanceAnalyzerThreads.RCA_SCHEDULER);
    rcaSchedulerThread.start();
    return scheduler;
  }

  private AppContext createAppContextWithDataNodes(String nodeName, NodeRole role, boolean isMaster) {
    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    List<ClusterDetailsEventProcessor.NodeDetails> nodes = new ArrayList<>();

    ClusterDetailsEventProcessor.NodeDetails node1 =
        new ClusterDetailsEventProcessor.NodeDetails(role, nodeName, "127.0.0.0", isMaster);
    nodes.add(node1);

    clusterDetailsEventProcessor.setNodesDetails(nodes);

    AppContext appContext = new AppContext();
    appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);
    return appContext;
  }

  /**
   * Add testPersistSummaryOnDataNode() and testPersistSummaryOnMasterNode() into a single UT
   * This will force both tests to run in sequential and can avoid access contention to the
   * same db file.
   * @throws Exception SQL exception
   */
  @Test
  public void testPersistSummary() throws Exception {
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());
    RcaConf masterRcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca_elected_master.conf").toString());
    Persistable persistable = PersistenceFactory.create(rcaConf);
    testPersistSummaryOnDataNode(rcaConf, persistable);
    persistable = PersistenceFactory.create(rcaConf);
    testPersistSummaryOnMasterNode(masterRcaConf, persistable);
  }

  private void testPersistSummaryOnDataNode(RcaConf rcaConf, Persistable persistable) throws Exception {
    AppContext appContext = createAppContextWithDataNodes("node1", NodeRole.DATA, false);

    AnalysisGraph graph = new DataNodeGraph();
    RCAScheduler scheduler = startScheduler(rcaConf, graph, persistable, this.queryable, appContext);
    // Wait at most 1 minute for the persisted data to show up with the correct contents
    WaitFor.waitFor(() -> {
      String readTableStr = persistable.read();
      System.out.println(readTableStr);
      if (readTableStr != null) {
        // HighHeapUsageClusterRcaX is a cluster level RCA so it should not be scheduled and persisted on
        // data node.
        return readTableStr.contains("HotResourceSummary") && readTableStr.contains("DummyYoungGenRca")
                && readTableStr.contains("HotNodeSummary") && readTableStr.contains("HotNodeRcaX");
      }
      return false;
    }, 1, TimeUnit.MINUTES);
    scheduler.shutdown();
  }

  private void testPersistSummaryOnMasterNode(RcaConf rcaConf, Persistable persistable) throws Exception {
    AppContext appContext = createAppContextWithDataNodes("node1", NodeRole.DATA, true);
    AnalysisGraph graph = new MasterNodeGraph();
    RCAScheduler scheduler = startScheduler(rcaConf, graph, persistable, this.queryable, appContext);
    // Wait at most 1 minute for the persisted data to show up with the correct contents
    WaitFor.waitFor(() -> {
      String readTableStr = persistable.read();
      if (readTableStr != null) {
        return readTableStr.contains("HotResourceSummary") && readTableStr.contains("DummyYoungGenRca")
                && readTableStr.contains("HotNodeSummary") && readTableStr.contains("HotNodeRcaX")
                && readTableStr.contains("HighHeapUsageClusterRcaX");
      }
      return false;
    }, 1, TimeUnit.MINUTES);
    scheduler.shutdown();
  }

  @After
  public void cleanup() throws Exception {
    if (queryable != null) {
      queryable.getMetricsDB().close();
      queryable.getMetricsDB().deleteOnDiskFile();
    }
  }
}
