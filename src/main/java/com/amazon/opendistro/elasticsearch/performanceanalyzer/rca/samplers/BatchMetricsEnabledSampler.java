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
