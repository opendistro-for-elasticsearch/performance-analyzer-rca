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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.HTTP_RequestDocs;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.MetricsDBProviderTestHelper;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class MetricTest {

  class MetricX extends HTTP_RequestDocs {
    public MetricX() {
      super(5);
    }

    public void setFlowUnitList(List<MetricFlowUnit> flowUnitList) {
      setFlowUnits(flowUnitList);
    }
  }

  @Test
  public void gather() throws Exception {
    Queryable queryable = new MetricsDBProviderTestHelper(false);
    Metric metric = new MetricX();
    MetricFlowUnit mfu = metric.gather(queryable);
    Assert.assertTrue(mfu.isEmpty());
  }
}