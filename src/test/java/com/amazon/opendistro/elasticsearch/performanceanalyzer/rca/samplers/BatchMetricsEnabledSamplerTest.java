/*
 *  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.samplers;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ReaderMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.collectors.SampleAggregator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class BatchMetricsEnabledSamplerTest {
  private static Path batchMetricsEnabledConfFile;
  private static String rootLocation;
  private static ReaderMetricsProcessor mp;
  private static AppContext appContext;
  private static BatchMetricsEnabledSampler uut;

  @Mock
  private SampleAggregator sampleAggregator;

  @BeforeClass
  public static void setUpClass() throws Exception {
    Files.createDirectories(Paths.get(Util.DATA_DIR));
    batchMetricsEnabledConfFile = Paths.get(Util.DATA_DIR, ReaderMetricsProcessor.BATCH_METRICS_ENABLED_CONF_FILE);
    Files.deleteIfExists(batchMetricsEnabledConfFile);

    rootLocation = "build/resources/test/reader/";
    mp = new ReaderMetricsProcessor(rootLocation);
    ReaderMetricsProcessor.setCurrentInstance(mp);

    appContext = new AppContext();
    uut = new BatchMetricsEnabledSampler(appContext);
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  private void writeBatchMetricsEnabled(boolean enabled) throws IOException {
    Files.write(batchMetricsEnabledConfFile, Boolean.toString(enabled).getBytes());
  }

  private void clearBatchMetricsEnabled() throws IOException {
    Files.deleteIfExists(batchMetricsEnabledConfFile);
  }

  @Test
  public void testIsBatchMetricsEnabled_notMaster() {
    appContext.setClusterDetailsEventProcessor(null);
    assertFalse(uut.isBatchMetricsEnabled());
  }

  @Test
  public void testIsBatchMetricsEnabled() throws IOException {
    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    ClusterDetailsEventProcessor.NodeDetails details =
        ClusterDetailsEventProcessorTestHelper.newNodeDetails("nodex", "127.0.0.1", true);
    clusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(details));
    appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);

    // No batch metrics enabled file
    clearBatchMetricsEnabled();
    mp.readBatchMetricsEnabledFromConfShim();
    assertFalse(uut.isBatchMetricsEnabled());

    // Batch metrics disabled
    writeBatchMetricsEnabled(false);
    mp.readBatchMetricsEnabledFromConfShim();
    assertFalse(uut.isBatchMetricsEnabled());

    // Batch metrics disabled
    writeBatchMetricsEnabled(true);
    mp.readBatchMetricsEnabledFromConfShim();
    assertTrue(uut.isBatchMetricsEnabled());
  }

  @Test
  public void testSample() {
    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    ClusterDetailsEventProcessor.NodeDetails details =
        ClusterDetailsEventProcessorTestHelper.newNodeDetails("nodex", "127.0.0.1", true);
    clusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(details));
    appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);

    uut.sample(sampleAggregator);
    verify(sampleAggregator, times(1))
        .updateStat(ReaderMetrics.BATCH_METRICS_ENABLED, "",
            mp.getBatchMetricsEnabled() ? 1 : 0);
  }
}
