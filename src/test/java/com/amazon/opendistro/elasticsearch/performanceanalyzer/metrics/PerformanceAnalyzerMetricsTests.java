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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics;

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

@PowerMockIgnore({"org.apache.logging.log4j.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({PerformanceAnalyzerMetrics.class, PluginSettings.class})
@SuppressStaticInitializationFor({"PluginSettings"})
public class PerformanceAnalyzerMetricsTests {

  @Before
  public void setUp() throws Exception {
    PluginSettings config = Mockito.mock(PluginSettings.class);
    Mockito.when(config.getMetricsLocation()).thenReturn("/dev/shm/performanceanalyzer");
    Mockito.when(config.getWriterQueueSize()).thenReturn(1);
    PowerMockito.mockStatic(PluginSettings.class);
    PowerMockito.when(PluginSettings.instance()).thenReturn(config);
  }

  // @Test
  public void testBasicMetric() {
    System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
    PerformanceAnalyzerMetrics.emitMetric(
        System.currentTimeMillis(),
            PluginSettings.instance().getMetricsLocation() + "/dir1/test1",
        "value1");
    assertEquals(
        "value1",
        PerformanceAnalyzerMetrics.getMetric(
                PluginSettings.instance().getMetricsLocation() + "/dir1/test1"));

    assertEquals(
        "",
        PerformanceAnalyzerMetrics.getMetric(
                PluginSettings.instance().getMetricsLocation() + "/dir1/test2"));

    PerformanceAnalyzerMetrics.removeMetrics(PluginSettings.instance().getMetricsLocation() + "/dir1");
  }

  // TODO: Turn it on later
  @Ignore
  public void testGeneratePath() {
    long startTimeInMillis = 1553725339;
    String generatedPath =
        PerformanceAnalyzerMetrics.generatePath(startTimeInMillis, "dir1", "id", "dir2");
    String expectedPath =
            PluginSettings.instance().getMetricsLocation()
            + "/"
            + String.valueOf(PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMillis))
            + "/dir1/id/dir2";
    assertEquals(expectedPath, generatedPath);
  }
}
