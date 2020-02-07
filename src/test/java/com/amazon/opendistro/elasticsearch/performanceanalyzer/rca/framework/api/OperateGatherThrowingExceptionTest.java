/*
 *  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.SymptomFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.HTTP_RequestDocs;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.MetricsDBProviderTestHelper;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class OperateGatherThrowingExceptionTest {

  class MetricX extends HTTP_RequestDocs {
    public MetricX() {
      super(5);
    }

    public void setFlowUnitList(List<MetricFlowUnit> flowUnitList) {
      setFlowUnits(flowUnitList);
    }
  }

  @Test
  public void queryUnavailableMetric() throws Exception {
    Queryable queryable = new MetricsDBProviderTestHelper(false);
    Metric metric = new MetricX();
    MetricFlowUnit mfu = metric.gather(queryable);
    Assert.assertTrue(mfu.isEmpty());
  }

  class MetricY extends Metric {
    public MetricY() {
      super("MetricY", 5);
    }
  }

  class QueryableImpl implements Queryable {

    @Override
    public MetricsDB getMetricsDB() {
      return null;
    }

    @Override
    public List<List<String>> queryMetrics(MetricsDB db, String metricName) {
      throw new RuntimeException();
    }

    @Override
    public List<List<String>> queryMetrics(
        MetricsDB db, String metricName, String dimension, String aggregation) throws Exception {
      return null;
    }

    @Override
    public long getDBTimestamp(MetricsDB db) {
      return 0;
    }
  }

  @Test
  public void gatherThrowsRuntimeException() throws Exception {
    Queryable queryable = new QueryableImpl();
    Metric metric = new MetricY();
    System.out.println("This exception is thrown as part of the test. This is expected");
    MetricFlowUnit mfu = metric.gather(queryable);
    Assert.assertTrue(mfu.isEmpty());
  }

  class SymptomX extends Symptom {

    public SymptomX() {
      super(5);
    }

    @Override
    public SymptomFlowUnit operate() {
      throw new RuntimeException();
    }
  }

  @Test
  public void emptySymptomFuOnException() {
    SymptomX symptom = new SymptomX();

    System.out.println("This exception is thrown as part of the test. This is expected");
    symptom.generateFlowUnitListFromLocal(null);
    Assert.assertEquals(1, symptom.getFlowUnits().size());
    Assert.assertTrue(symptom.getFlowUnits().get(0).isEmpty());
  }

  class RcaX extends Rca<ResourceFlowUnit> {

    public RcaX() {
      super(5);
    }

    @Override
    public ResourceFlowUnit operate() {
      throw new RuntimeException();
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {}
  }

  @Test
  public void emptyRcaFuOnException() {
    RcaX rca = new RcaX();
    System.out.println("This exception is thrown as part of the test. This is expected");
    rca.generateFlowUnitListFromLocal(null);
    Assert.assertEquals(1, rca.getFlowUnits().size());
    Assert.assertTrue(rca.getFlowUnits().get(0).isEmpty());
  }
}
