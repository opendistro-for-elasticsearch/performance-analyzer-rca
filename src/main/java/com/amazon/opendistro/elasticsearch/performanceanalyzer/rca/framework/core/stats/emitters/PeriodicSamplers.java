package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.emitters;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.collectors.aggregator.SampleAggregator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.sampled.JvmMeasurements;
import java.util.Arrays;
import java.util.List;

public class PeriodicSamplers implements Runnable {
  private List<Sampler> allSamplers;
  private long samplingStartMillis;
  private long samplingEndMillis;

  private final SampleAggregator sampleCollector;

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
