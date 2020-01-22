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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.emitters;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.collectors.aggregator.SampleAggregator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.sampled.JvmMeasurements;
import java.util.Arrays;
import java.util.List;

public class PeriodicSamplers implements Runnable {
  private final SampleAggregator sampleCollector;
  private List<Sampler> allSamplers;
  private long samplingStartMillis;
  private long samplingEndMillis;

  public PeriodicSamplers(SampleAggregator sampleCollector) {
    this.sampleCollector = sampleCollector;
    allSamplers =
        Arrays.asList(new JvmFreeMemSampler(), new JvmTotalMemSampler(), new ThreadCount());
    samplingStartMillis = 0L;
    samplingEndMillis = 0L;
  }

  @Override
  public void run() {
    samplingStartMillis = System.currentTimeMillis();
    for (Sampler sampler : allSamplers) {
      sampler.sample(sampleCollector);
    }
    samplingEndMillis = System.currentTimeMillis();
  }

  interface Sampler {
    void sample(SampleAggregator sampleCollector);
  }

  /** Total amount of free memory available to the JVM. */
  class JvmFreeMemSampler implements Sampler {
    @Override
    public void sample(SampleAggregator collector) {
      collector.updateStat(
          JvmMeasurements.JVM_FREE_MEM_SAMPLER, "", Runtime.getRuntime().freeMemory());
    }
  }

  /** Total memeory available to the JVM at the moment. Might vary over time. */
  class JvmTotalMemSampler implements Sampler {
    @Override
    public void sample(SampleAggregator sampleCollector) {
      sampleCollector.updateStat(
          JvmMeasurements.JVM_TOTAL_MEM_SAMPLER, "", Runtime.getRuntime().totalMemory());
    }
  }

  /** Gives a count of all threads in the JVM */
  class ThreadCount implements Sampler {
    @Override
    public void sample(SampleAggregator sampleCollector) {
      sampleCollector.updateStat(
          JvmMeasurements.THREAD_COUNT, "", Thread.getAllStackTraces().keySet().size());
    }
  }
}
