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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import org.jooq.Record;

public class SummaryBuilder<T extends GenericSummary> {
  private final String tableName;
  SummaryBuilderFunction<T> builder;

  public SummaryBuilder(final String tableName, SummaryBuilderFunction<T> builder) {
    this.tableName = tableName;
    this.builder = builder;
  }

  public GenericSummary buildSummary(Record record) {
    return builder.buildSummary(record);
  }

  public String getTableName() {
    return tableName;
  }
}
