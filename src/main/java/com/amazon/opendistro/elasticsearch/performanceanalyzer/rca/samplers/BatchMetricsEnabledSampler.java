package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.samplers;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ReaderMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.collectors.SampleAggregator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.emitters.ISampler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor;

import java.util.Objects;

public class BatchMetricsEnabledSampler implements ISampler {
  private final AppContext appContext;

  public BatchMetricsEnabledSampler(final AppContext appContext) {
    Objects.requireNonNull(appContext);
    this.appContext = appContext;
  }

  @Override
  public void sample(SampleAggregator sampleCollector) {
    sampleCollector.updateStat(ReaderMetrics.BATCH_METRICS_ENABLED, "",
        isBatchMetricsEnabled() ? 1 : 0);
  }

  boolean isBatchMetricsEnabled() {
    InstanceDetails currentNode = appContext.getMyInstanceDetails();
    if (currentNode != null && currentNode.getIsMaster()) {
      return ReaderMetricsProcessor.getInstance().getBatchMetricsEnabled();
    }
    return false;
  }
}
