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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.LeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;

public abstract class Metric extends LeafNode<MetricFlowUnit> {
  static final String[] metricList;

  static {
    AllMetrics.OSMetrics[] osMetrics = AllMetrics.OSMetrics.values();
    metricList = new String[osMetrics.length];
    for (int i = 0; i < osMetrics.length; ++i) {
      metricList[i] = osMetrics[i].name();
    }
  }

  private String name;
  private static final Logger LOG = LogManager.getLogger(Metric.class);

  public Metric(String name, long evaluationIntervalSeconds) {
    super(0, evaluationIntervalSeconds);
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  public MetricFlowUnit gather(Queryable queryable) {
    LOG.debug("Trying to gather metrics for {}", name);
    MetricsDB db;
    try {
      db = queryable.getMetricsDB();
    } catch (Exception e) {
      PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
          ExceptionsAndErrors.EXCEPTION_IN_GATHER, name(), 1);
      // TODO: Emit log/stats that gathering failed.
      LOG.error("RCA: Caught an exception while getting the DB {}", e.getMessage());
      e.printStackTrace();
      return MetricFlowUnit.generic();
    }
    try {
      Result<Record> result = queryable.queryMetrics(db, name);
      return new MetricFlowUnit(queryable.getDBTimestamp(db), result);
    } catch (DataAccessException dex) {
      // This can happen if the RCA started querying for metrics before the Reader obtained them.
      // This is not an error.
      LOG.info("Looking for metric {}, when it does not exist.", name);
    } catch (Exception e) {
      PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
          ExceptionsAndErrors.EXCEPTION_IN_GATHER, name(), 1);
      e.printStackTrace();
      LOG.error("Metric exception: {}", e.getMessage());
    }
    return MetricFlowUnit.generic();
  }

  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    long startTime = System.currentTimeMillis();
    MetricFlowUnit mfu = gather(args.getQueryable());
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR.updateStat(
        RcaGraphMetrics.METRIC_GATHER_CALL, this.name(), duration);
    setFlowUnits(Collections.singletonList(mfu));
  }

  /**
   * Persists the given flow unit.
   *
   * @param args The arg wrapper.
   */
  @Override
  public void persistFlowUnit(FlowUnitOperationArgWrapper args) {}

  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    LOG.error("we are not supposed to read metric flowunit from wire.");
  }

  /**
   * This method specifies what needs to be done when the current node is muted for throwing
   * exceptions.
   */
  @Override
  public void handleNodeMuted() {
    setLocalFlowUnit(MetricFlowUnit.generic());
  }
}
