/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.flow_units;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import java.util.ArrayList;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.Mock;
import org.jooq.tools.jdbc.MockConnection;

public class MetricFlowUnitTestHelper {

  public static MetricFlowUnit createFlowUnit(final List<String> fieldName, final List<String>... rows) {
    DSLContext context = DSL.using(new MockConnection(Mock.of(0)));
    List<String[]> stringData = new ArrayList<>();
    stringData.add(fieldName.toArray(new String[0]));
    for (int i = 0; i < rows.length; i++) {
      stringData.add(rows[i].toArray(new String[0]));
    }
    Result<Record> newRecordList = context.fetchFromStringData(stringData);
    return new MetricFlowUnit(0, newRecordList);
  }
}
