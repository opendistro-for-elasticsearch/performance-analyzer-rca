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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.TopConsumerSummaryMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import java.util.ArrayList;
import java.util.List;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * TopConsumerSummary contains the name and usage of a resource consumer.
 */
public class TopConsumerSummary extends GenericSummary {

  private final String name;
  private final double value;

  public TopConsumerSummary(final String name, final double value) {
    super();
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return this.name;
  }

  public double getValue() {
    return this.value;
  }

  @Override
  public TopConsumerSummaryMessage buildSummaryMessage() {
    final TopConsumerSummaryMessage.Builder summaryMessageBuilder = TopConsumerSummaryMessage.newBuilder();
    summaryMessageBuilder.setName(this.name);
    summaryMessageBuilder.setValue(this.value);
    return summaryMessageBuilder.build();
  }

  /**
   * TopConsumerSummary is the lowest level summary in the nest sunmmary hierarchy.
   * So it doesn't carry any nested summary list and thus we override this with a empty method here.
   * @param messageBuilder
   */
  @Override
  public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
  }

  public static TopConsumerSummary buildTopConsumerSummaryFromMessage(TopConsumerSummaryMessage message) {
    TopConsumerSummary newSummary = new TopConsumerSummary(message.getName(), message.getValue());
    return newSummary;
  }

  @Override
  public String toString() {
    return this.name + " " + this.value;
  }

  @Override
  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schema = new ArrayList<>();
    schema.add(DSL.field(DSL.name(TopConsumerSummary.SQL_SCHEMA_CONSTANTS.CONSUMER_NAME_COL_NAME), String.class));
    schema.add(DSL.field(DSL.name(TopConsumerSummary.SQL_SCHEMA_CONSTANTS.CONSUMER_VALUE_COL_NAME), Double.class));
    return schema;
  }

  @Override
  public List<Object> getSqlValue() {
    List<Object> value = new ArrayList<>();
    value.add(this.name);
    value.add(this.value);
    return value;
  }

  public static class SQL_SCHEMA_CONSTANTS {

    public static final String CONSUMER_NAME_COL_NAME = "Name";
    public static final String CONSUMER_VALUE_COL_NAME = "Value";
  }
}
