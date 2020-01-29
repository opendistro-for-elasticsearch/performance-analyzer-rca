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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.DisksCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.HeapMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MetricsPurgeActivity;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NetworkE2ECollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NetworkInterfaceCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm.GCMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm.HeapMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm.ThreadList;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.OSGlobals;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.ThreadCPU;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.ThreadDiskIO;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.ThreadSched;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MetricsConfiguration {
  public static final int SAMPLING_INTERVAL = 5000;
  public static final int ROTATION_INTERVAL = 30000;
  public static final int STATS_ROTATION_INTERVAL = 60000;
  public static final int DELETION_INTERVAL =
      PluginSettings.instance().getMetricsDeletionInterval();

  public static class MetricConfig {
    public int samplingInterval;
    public int rotationInterval;
    public int deletionInterval;

    public MetricConfig(int samplingInterval, int rotationInterval, int deletionInterval) {
      this.samplingInterval = samplingInterval;
      this.rotationInterval = rotationInterval;
      this.deletionInterval = deletionInterval;
    }
  }

  public static final Map<Class, MetricConfig> CONFIG_MAP;
  public static final MetricConfig cdefault;

  static {
    cdefault = new MetricConfig(SAMPLING_INTERVAL, 0, 0);

    Map<Class, MetricConfig> configMapInitializer = new HashMap<>();

    configMapInitializer.put(ThreadCPU.class, cdefault);
    configMapInitializer.put(ThreadDiskIO.class, cdefault);
    configMapInitializer.put(ThreadSched.class, cdefault);
    configMapInitializer.put(ThreadList.class, cdefault);
    configMapInitializer.put(GCMetrics.class, cdefault);
    configMapInitializer.put(HeapMetrics.class, cdefault);
    configMapInitializer.put(NetworkE2ECollector.class, cdefault);
    configMapInitializer.put(NetworkInterfaceCollector.class, cdefault);
    configMapInitializer.put(OSGlobals.class, cdefault);
    configMapInitializer.put(PerformanceAnalyzerMetrics.class, new MetricConfig(0, ROTATION_INTERVAL, 0));
    configMapInitializer.put(
        MetricsPurgeActivity.class, new MetricConfig(ROTATION_INTERVAL, 0, DELETION_INTERVAL));
    configMapInitializer.put(StatsCollector.class, new MetricConfig(STATS_ROTATION_INTERVAL, 0, 0));
    configMapInitializer.put(DisksCollector.class, cdefault);
    configMapInitializer.put(HeapMetricsCollector.class, cdefault);

    CONFIG_MAP = Collections.unmodifiableMap(configMapInitializer);
  }
}
