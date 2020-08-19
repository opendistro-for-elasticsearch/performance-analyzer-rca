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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.CoolOffDetector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.FlipFlopDetector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.PersistableAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.TimedFlipFlopDetector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.PublisherEventFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Publisher extends NonLeafNode<PublisherEventFlowUnit> {

  private static final Logger LOG = LogManager.getLogger(Publisher.class);
  public static final String NAME = "Publisher";
  private Collator collator;
  private FlipFlopDetector flipFlopDetector;
  private boolean isMuted = false;
  private CoolOffDetector coolOffDetector;
  private List<ActionListener> actionListeners;
  protected Clock clock;

  public Publisher(int evalIntervalSeconds, Collator collator) {
    super(0, evalIntervalSeconds);
    this.collator = collator;
    this.actionListeners = new ArrayList<>();
    this.coolOffDetector = new CoolOffDetector();
    // TODO please bring in guice so we can configure this with DI
    this.flipFlopDetector = new TimedFlipFlopDetector(1, TimeUnit.HOURS);
    this.clock = Clock.systemUTC();
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PublisherEventFlowUnit operate() {
    // TODO: Need to add dampening, avoidance, state persistence etc.
    Decision decision = collator.getFlowUnits().get(0);
    List<PersistableAction> persistableActions = new ArrayList<>();
    for (Action action : decision.getActions()) {
      if (coolOffDetector.isCooledOff(action) && !flipFlopDetector.isFlipFlop(action)) {
        flipFlopDetector.recordAction(action);
        coolOffDetector.recordAction(action);
        for (ActionListener listener : actionListeners) {
          listener.actionPublished(action);
        }
        if (action instanceof PersistableAction) {
          persistableActions.add((PersistableAction) action);
        }
      }
    }
    long timestamp = clock.millis();
    if (!persistableActions.isEmpty()) {
      return new PublisherEventFlowUnit(timestamp, new PublisherEvent(name(), timestamp, persistableActions));
    }
    else {
      return new PublisherEventFlowUnit(timestamp);
    }
  }

  @Override
  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    LOG.debug("Publisher: Executing fromLocal: {}", name());
    long startTime = System.currentTimeMillis();

    try {
      this.operate();
    } catch (Exception ex) {
      LOG.error("Publisher: Exception in operate", ex);
      PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
          ExceptionsAndErrors.EXCEPTION_IN_OPERATE, name(), 1);
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

  @VisibleForTesting
  protected CoolOffDetector getCoolOffDetector() {
    return this.coolOffDetector;
  }

  @VisibleForTesting
  public void setClock(Clock clock) {
    this.clock = clock;
  }
}
