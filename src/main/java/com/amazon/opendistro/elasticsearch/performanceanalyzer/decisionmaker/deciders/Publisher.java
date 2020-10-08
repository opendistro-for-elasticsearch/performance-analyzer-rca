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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ActionListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.FlipFlopDetector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.TimedFlipFlopDetector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.collator.Collator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PublisherEventsPersistor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Publisher extends NonLeafNode<EmptyFlowUnit> {

  private static final Logger LOG = LogManager.getLogger(Publisher.class);

  private Collator collator;
  private FlipFlopDetector flipFlopDetector;
  private boolean isMuted = false;
  private List<ActionListener> actionListeners;

  public Publisher(int evalIntervalSeconds, Collator collator) {
    super(0, evalIntervalSeconds);
    this.collator = collator;
    this.actionListeners = new ArrayList<>();
    // TODO please bring in guice so we can configure this with DI
    this.flipFlopDetector = new TimedFlipFlopDetector(1, TimeUnit.HOURS);
  }

  @Override
  public EmptyFlowUnit operate() {
    return new EmptyFlowUnit(Instant.now().toEpochMilli());
  }

  /**
   * Publish the decisions to action listeners and persist actions.
   */
  public void compute(FlowUnitOperationArgWrapper args) {
    // TODO: Need to add dampening, avoidance etc.
    List<Action> actionsPublished = new ArrayList<>();
    if (!collator.getFlowUnits().isEmpty()) {
      Decision decision = collator.getFlowUnits().get(0);
      for (Action action : decision.getActions()) {
        if (!flipFlopDetector.isFlipFlop(action)) {
          flipFlopDetector.recordAction(action);
          for (ActionListener listener : actionListeners) {
            listener.actionPublished(action);
          }
          actionsPublished.add(action);
        }
      }
    }
    // Persist actions to sqlite
    PublisherEventsPersistor persistor = new PublisherEventsPersistor(args.getPersistable());
    persistor.persistAction(actionsPublished);
  }

  @Override
  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    LOG.debug("Publisher: Executing fromLocal: {}", name());
    long startTime = System.currentTimeMillis();

    try {
      this.compute(args);
    } catch (Exception ex) {
      LOG.error("Publisher: Exception in compute", ex);
      PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
          ExceptionsAndErrors.EXCEPTION_IN_COMPUTE, name(), 1);
    }
    long duration = System.currentTimeMillis() - startTime;

    PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR.updateStat(
        RcaGraphMetrics.GRAPH_NODE_OPERATE_CALL, this.name(), duration);
  }

  /**
   * Register an action listener with Publisher
   *
   * <p>The listener is notified whenever an action is published
   */
  public void addActionListener(ActionListener listener) {
    actionListeners.add(listener);
  }

  /**
   * Publisher does not have downstream nodes and does not emit flow units
   */
  @Override
  public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
    assert true;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    assert true;
  }

  @Override
  public void handleNodeMuted() {
    assert true;
  }

  @VisibleForTesting
  protected FlipFlopDetector getFlipFlopDetector() {
    return this.flipFlopDetector;
  }
}
