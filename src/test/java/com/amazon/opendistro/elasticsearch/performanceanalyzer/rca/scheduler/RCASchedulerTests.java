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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ThresholdMain;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PersistenceFactory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.MetricsDBProviderTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageOldGenRca;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
@SuppressWarnings("serial")
public class RCASchedulerTests {
  private static final Logger LOG = LogManager.getLogger(Tasklet.class);
  Queryable queryable;

  @Test
  public void testScheduler() throws Exception {
    // First test
    AnalysisGraph graph =
        new AnalysisGraph() {
          @Override
          public void construct() {
            Metric heapUsed = new Heap_Used(5);
            Metric gcEvent = new GC_Collection_Event(5);
            Metric heapMax = new Heap_Max(5);

            addLeaf(heapUsed);
            addLeaf(gcEvent);
            addLeaf(heapMax);
            Rca highHeapUsageRca = new HighHeapUsageOldGenRca(2L, heapUsed, gcEvent, heapMax);
            highHeapUsageRca.addAllUpstreams(Arrays.asList(heapMax, heapUsed, gcEvent));
          }
        };

    queryable = new MetricsDBProviderTestHelper(false);

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
    scheduler.start();
    Thread.sleep(8000);

    LOG.info("About to send shutdown signal from the test ..");
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
