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

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.MovingAverage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.SymptomContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.SymptomFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.MetricsDBProviderTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.helpers.OSMetricHelper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class HighCpuSymptomTest {
  static class CPU_UtilizationX extends CPU_Utilization {
    public CPU_UtilizationX(long evaluationIntervalSeconds) {
      super(evaluationIntervalSeconds);
    }

    public void setFlowUnitList(List<MetricFlowUnit> flowUnitList) {
      setFlowUnits(flowUnitList);
    }
  }

  static class ShardCpuHighSymptom extends Symptom {
    private final Metric cpu_UtilizationX;

    public <M extends Metric> ShardCpuHighSymptom(
        long evaluationIntervalSeconds, M cpu_UtilizationX) {
      super(evaluationIntervalSeconds);
      this.cpu_UtilizationX = cpu_UtilizationX;
    }

    // The operate here tries to find hot shard. The way it tries to do it is creates a
    // MovingAverage object for
    // each distinct shardID. The moving average window is defined as three samples for less that
    // three it
    // outputs -1 and for greater than or equal to 3, it outputs the average for that window. This
    // method
    // compares it against an arbitrary threshold of 90% and if such a shard exists, then it reports
    // it as a flow
    // unit. So the symptom can be defined as if there exists a shard for which the average of max
    // is greater
    // than 90% for three consecutive samples, then classify it as hot. This is not accurate in
    // production but it
    // tests the system works for arbitrary samples of data.
    @Override
    public SymptomFlowUnit operate() {
      List<MetricFlowUnit> cpuMetrics = cpu_UtilizationX.getFlowUnits();
      boolean shouldReportOperation = false;

      MetricFlowUnit cpuMetric = cpuMetrics.get(0);
      List<List<String>> allData = cpuMetric.getData();
      List<String> cols = allData.get(0);
      int shardIDIdx = -1;
      int maxColIdx = -1;

      // Get the index of the shardID column.
      for (int i = 0; i < cols.size(); i++) {
        if (cols.get(i).equals(OSMetricHelper.getDims().get(0))) {
          shardIDIdx = i;
          break;
        }
      }

      // Get the index of the max column.
      for (int i = 0; i < cols.size(); i++) {
        if (cols.get(i).equals(MetricsDB.MAX)) {
          maxColIdx = i;
          break;
        }
      }

      Map<String, MovingAverage> averageMap = new HashMap<>();
      final double HIGH_CPU_THRESHOLD = 90.0;
      List<List<String>> ret = new ArrayList<>();
      // The first row is the column names, so we start from the row 1.
      for (int i = 1; i < allData.size(); i++) {
        List<String> row = allData.get(i);
        String shardId = row.get(shardIDIdx);
        MovingAverage entry = averageMap.get(shardId);
        if (null == entry) {
          entry = new MovingAverage(3);
          averageMap.put(shardId, entry);
        }
        double val = entry.next(Double.parseDouble(row.get(maxColIdx)));
        if (val > HIGH_CPU_THRESHOLD) {
          List<String> dataRow = Collections.singletonList(shardId);
          // context.put("threshold", String.valueOf(HIGH_CPU_THRESHOLD));
          // context.put("actual", String.valueOf(val));
          ret.add(dataRow);
          System.out.println(
              String.format(
                  "Shard %s is hot. Average max CPU (%f) above: %f",
                  shardId, val, HIGH_CPU_THRESHOLD));
          shouldReportOperation = true;
        }
      }

      if (shouldReportOperation) {
        return new SymptomFlowUnit(
            System.currentTimeMillis(), ret, new SymptomContext(SymptomContext.State.UNHEALTHY));
      } else {
        return new SymptomFlowUnit(
            System.currentTimeMillis(), new SymptomContext(SymptomContext.State.HEALTHY));
      }
    }
  }

  @Test
  public void testSymptomCreation() throws Exception {
    AnalysisGraph graph =
        new AnalysisGraph() {
          @Override
          public void construct() {
            Metric metric = new CPU_UtilizationX(60);
            addLeaf(metric);
            Symptom symptom = new ShardCpuHighSymptom(60, metric);
            symptom.addAllUpstreams(Collections.singletonList(metric));
          }
        };

    List<ConnectedComponent> components = RcaUtil.getAnalysisGraphComponents(graph);
    Queryable queryable = new MetricsDBProviderTestHelper(false);

    ((MetricsDBProviderTestHelper) queryable)
        .addNewData(
            CPU_Utilization.NAME, Arrays.asList("shard1", "index3", "bulk", "primary"), 92.4);
    ((MetricsDBProviderTestHelper) queryable)
        .addNewData(
            CPU_Utilization.NAME, Arrays.asList("shard2", "index3", "bulk", "primary"), 93.4);
    ((MetricsDBProviderTestHelper) queryable)
        .addNewData(
            CPU_Utilization.NAME, Arrays.asList("shard1", "index3", "bulk", "primary"), 95.4);
    ((MetricsDBProviderTestHelper) queryable)
        .addNewData(
            CPU_Utilization.NAME, Arrays.asList("shard2", "index3", "bulk", "primary"), 4.4);
    ((MetricsDBProviderTestHelper) queryable)
        .addNewData(
            CPU_Utilization.NAME, Arrays.asList("shard1", "index3", "bulk", "primary"), 90.4);
    ((MetricsDBProviderTestHelper) queryable)
        .addNewData(
            CPU_Utilization.NAME, Arrays.asList("shard2", "index3", "bulk", "primary"), 5.4);

    for (ConnectedComponent component : components) {
      for (List<Node<?>> nodeList : component.getAllNodesByDependencyOrder()) {
        for (Node<?> node : nodeList) {
          if (node instanceof Metric) {
            List<MetricFlowUnit> flowUnits =
                Collections.singletonList(((Metric) node).gather(queryable));
            ((CPU_UtilizationX) node).setFlowUnitList(flowUnits);
          } else if (node instanceof Symptom) {
            SymptomFlowUnit flowUnit = ((Symptom) node).operate();
            assertEquals(flowUnit.getData().get(0), Collections.singletonList("shard1"));
            assertEquals(flowUnit.getContext().getState(), SymptomContext.State.UNHEALTHY);
            /*
            AssertHelper.compareMaps(new HashMap<String, String>() {{
                this.put("threshold", "90.0");
                this.put("actual", "92.7");
            }}, flowUnit.getContextMap());
             */
          }
        }
      }
    }
  }
}
