package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.emitters;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.collectors.samplers.SampleCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.sampled.LivenessMeasurements;
import java.util.Arrays;
import java.util.List;

public class PeriodicSamplers implements Runnable {
  private List<Sampler> allSamplers;
  private long samplingStartMillis;
  private long samplingEndMillis;

  private final SampleCollector sampleCollector;

  public PeriodicSamplers(SampleCollector sampleCollector) {
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
    void sample(SampleCollector sampleCollector);
  }

  /** Total amount of free memory available to the JVM. */
  class JvmFreeMemSampler implements Sampler {
    @Override
    public void sample(SampleCollector collector) {
      collector.updateSample(
              LivenessMeasurements.JVM_FREE_MEM_SAMPLER, Runtime.getRuntime().freeMemory());
    }
  }

  /** Total memeory available to the JVM at the moment. Might vary over time. */
  class JvmTotalMemSampler implements Sampler {
    @Override
    public void sample(SampleCollector sampleCollector) {
      sampleCollector.updateSample(LivenessMeasurements.JVM_TOTAL_MEM_SAMPLER,
              Runtime.getRuntime().totalMemory());
    }
  }

  /** Gives a count of all threads in the JVM */
  class ThreadCount implements Sampler {
    @Override
    public void sample(SampleCollector sampleCollector) {
      sampleCollector.updateSample(LivenessMeasurements.THREAD_COUNT,
              Thread.getAllStackTraces().keySet().size());
    }
  }
}
