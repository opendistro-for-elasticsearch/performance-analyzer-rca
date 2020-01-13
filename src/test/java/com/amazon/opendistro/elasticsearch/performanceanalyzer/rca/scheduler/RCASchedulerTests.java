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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_AllocRate;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Paging_MajfltRate;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Sched_Waittime;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ThresholdMain;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PersistenceFactory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.MetricsDBProviderTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageOldGenRca;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
@SuppressWarnings("serial")
public class RCASchedulerTests {
  Queryable queryable;

  class AnalysisGraphTest extends AnalysisGraph {

    @Override
    public void construct() {
      Metric cpuUtilization = new CPU_Utilization(5);
      Metric heapUsed = new Sched_Waittime(5);
      Metric pageMaj = new Paging_MajfltRate(5);
      Metric heapAlloc = new Heap_AllocRate(5);

      addLeaf(cpuUtilization);
      addLeaf(heapUsed);
      addLeaf(pageMaj);
      addLeaf(heapAlloc);
      Rca highHeapUsageRca = new HighHeapUsageOldGenRca(2L, 1, heapUsed, pageMaj, heapAlloc);
      highHeapUsageRca.addAllUpstreams(Arrays.asList(heapAlloc, heapUsed, pageMaj));
    }
  }

  @Before
  public void before() throws Exception {
    queryable = new MetricsDBProviderTestHelper(false);
  }

  @Test
  public void testScheduler() throws Exception {
    // First test
    AnalysisGraph graph = new AnalysisGraphTest();

    ((MetricsDBProviderTestHelper) queryable)
        .addNewData(
            CPU_Utilization.NAME, Arrays.asList("shard1", "index3", "bulk", "primary"), 92.4);
    ((MetricsDBProviderTestHelper) queryable)
        .addNewData(
            CPU_Utilization.NAME, Arrays.asList("shard2", "index3", "bulk", "primary"), 93.4);
    ((MetricsDBProviderTestHelper) queryable)
        .addNewData(
            CPU_Utilization.NAME, Arrays.asList("shard1", "index3", "bulk", "primary"), 95.4);
    ((MetricsDBProviderTestHelper) queryable)
        .addNewData(
            CPU_Utilization.NAME, Arrays.asList("shard2", "index3", "bulk", "primary"), 4.4);

    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());
    Persistable persistable = PersistenceFactory.create(rcaConf);
    RCAScheduler scheduler =
        new RCAScheduler(
            RcaUtil.getAnalysisGraphComponents(graph),
            queryable,
            rcaConf,
            new ThresholdMain(
                Paths.get(RcaConsts.TEST_CONFIG_PATH, "thresholds").toString(), rcaConf),
            persistable,
            new WireHopper(null, null, null, null));
    scheduler.setRole(AllMetrics.NodeRole.DATA);
    scheduler.start();
    Thread.sleep(8000);
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
