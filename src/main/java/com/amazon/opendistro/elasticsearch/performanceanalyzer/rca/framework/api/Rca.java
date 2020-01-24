/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class Rca<T extends ResourceFlowUnit> extends NonLeafNode<T> {
  private static final Logger LOG = LogManager.getLogger(Rca.class);

  public Rca(long evaluationIntervalSeconds) {
    super(0, evaluationIntervalSeconds);
  }

  /**
   * fetch flowunits from local graph node
   * @param args The wrapper around the flow unit operation.
   */
  @Override
  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    LOG.debug("rca: Executing fromLocal: {}", this.getClass().getSimpleName());

    long startTime = System.nanoTime();
    T out = this.operate();
    long endTime = System.nanoTime();
    long duration = (endTime - startTime) / 1000;

    PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR.updateStat(
            RcaGraphMetrics.GRAPH_NODE_OPERATE_CALL, this.name(), duration);

    setFlowUnits(Collections.singletonList(out));
  }

  @Override
  public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
    long startTime = System.nanoTime();
    for (final T flowUnit : getFlowUnits()) {
        args.getPersistable().write(this, flowUnit);
    }

    long endTime = System.nanoTime();
    long duration = (endTime - startTime) / 1000;
    PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR
            .updateStat(RcaGraphMetrics.RCA_PERSIST_CALL, this.name(), duration);
  }
}
