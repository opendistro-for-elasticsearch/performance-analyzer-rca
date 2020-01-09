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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.LeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
      // TODO: Emit log/stats that gathering failed.
      LOG.error("RCA: Caught an exception while getting the DB {}", e.getMessage());
      return MetricFlowUnit.generic();
    }
    // LOG.debug("RCA: Metrics from MetricsDB {}", result);
    try {
      List<List<String>> result = queryable.queryMetrics(db, name);
      // return new FlowUnit(queryable.getDBTimestamp(db), result, Collections.emptyMap());
      return new MetricFlowUnit(queryable.getDBTimestamp(db), result);
    } catch (Exception e) {
      LOG.error("Metric exception: {}", e.getMessage());
      return MetricFlowUnit.generic();
    }
    // LOG.debug("RCA: Metrics from MetricsDB {}", result);
  }

  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    setFlowUnits(Collections.singletonList(gather(args.getQueryable())));
  }

  /**
   * Persists the given flow unit.
   * @param args The arg wrapper.
   */
  @Override
  public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
  }

  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    final List<FlowUnitWrapper> flowUnitWrappers =
        args.getWireHopper().readFromWire(args.getNode());
    final List<MetricFlowUnit> flowUnitList = new ArrayList<>();
    LOG.debug(
        "rca: Executing fromWire: {}, received : {}",
        this.getClass().getSimpleName(),
        flowUnitWrappers.size());
    for (FlowUnitWrapper messageWrapper : flowUnitWrappers) {
      flowUnitList.add(MetricFlowUnit.buildFlowUnitFromWrapper(messageWrapper));
    }

    setFlowUnits(flowUnitList);
  }
}
