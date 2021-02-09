/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.Mock;
import org.jooq.tools.jdbc.MockConnection;

public class MetricTestHelper extends Metric {
  private static final String METRIC_TEST_HELPER_NAME = "Metric_test_Helper";
  private DSLContext context;

  public MetricTestHelper(long evaluationIntervalSeconds) {
    super(METRIC_TEST_HELPER_NAME, evaluationIntervalSeconds);
    context = DSL.using(new MockConnection(Mock.of(0)));
  }

  public MetricTestHelper(long evaluationIntervalSeconds, String name) {
    super(name, evaluationIntervalSeconds);
    context = DSL.using(new MockConnection(Mock.of(0)));
  }

  public void createTestFlowUnits(final List<String> fieldNames, final List<String> row) {
    Result<Record> newRecordList = createTestResult(fieldNames, row);
    this.flowUnits = Collections.singletonList(new MetricFlowUnit(0, newRecordList));
  }

  public void createTestFlowUnitsWithMultipleRows(final List<String> fieldName, final List<List<String>> rows) {
    List<String[]> stringData = new ArrayList<>();
    stringData.add(fieldName.toArray(new String[0]));
    for (List<String> row : rows) {
      stringData.add(row.toArray(new String[0]));
    }
    Result<Record> newRecordList = context.fetchFromStringData(stringData);
    this.flowUnits = Collections.singletonList(new MetricFlowUnit(0, newRecordList));
  }

  public Result<Record> createTestResult(final List<String> fieldNames, final List<String> row) {
    List<String[]> stringData = new ArrayList<>();
    stringData.add(fieldNames.toArray(new String[0]));
    stringData.add(row.toArray(new String[0]));
    return context.fetchFromStringData(stringData);
  }
}
