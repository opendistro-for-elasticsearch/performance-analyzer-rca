/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Generic base class outlining the basic structure of a Decider.
 *
 * <p>Deciders process observation summaries from RCA nodes to generate Action recommendations. Each
 * decider operates independent of other deciders. It subscribes to relevant RCAs and Metrics, to
 * create a candidate set of actions for unhealthy nodes in the cluster (as suggested by its
 * upstream RCAs).
 *
 * <p>{@link Decider} implementations should override operate() and return a {@link Decision} based
 * on the decider's evaluations.
 *
 * <p>A Decision can contain multiple actions. Each Action contains details about the Elasticsearch
 * nodes it touches and its impact on key resource metrics.
 */
public abstract class Decider extends NonLeafNode<Decision> {

  private static final Logger LOG = LogManager.getLogger(Decider.class);
  protected final int decisionFrequency; // Measured in terms of number of evaluationIntervalPeriods

  public Decider(long evalIntervalSeconds, int decisionFrequency) {
    super(0, evalIntervalSeconds);
    this.decisionFrequency = decisionFrequency;
  }

  public abstract String name();

  @Override
  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    LOG.debug("decider: Executing fromLocal: {}", name());
    long startTime = System.currentTimeMillis();

    Decision decision;
    try {
      decision = this.operate();
    } catch (Exception ex) {
      LOG.error("decider: Exception in operate", ex);
      PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
          ExceptionsAndErrors.EXCEPTION_IN_OPERATE, name(), 1);
      decision = new Decision(System.currentTimeMillis(), this.name());
    }
    long duration = System.currentTimeMillis() - startTime;

    PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR.updateStat(
        RcaGraphMetrics.GRAPH_NODE_OPERATE_CALL, this.name(), duration);

    setLocalFlowUnit(decision);
  }

  @Override
  public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
    // TODO: Persist Decisions taken by deciders to support queryable APIs and general bookkeeping
    // This is a no-op for now
    assert true;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new IllegalArgumentException(name() + ": not expected to be called over the wire");
  }

  @Override
  public void handleNodeMuted() {
    setLocalFlowUnit(new Decision(System.currentTimeMillis(), this.name()));
  }

  @Override
  public abstract Decision operate();
}
