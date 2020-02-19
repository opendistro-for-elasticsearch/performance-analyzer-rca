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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.HotResourceSummaryMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PANetworking;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType.ResourceTypeOneofCase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import java.util.ArrayList;
import java.util.List;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * HotResourceSummary contains information such as the name of the hot resource, the current value
 * threshold, etc. It also contains the top K consumers of this particular resource. It is created
 * by some RCAs who work directly on some type of resource(JVM, CPU etc.)
 *
 * <p>This object is persisted in SQLite table
 * Table name : HotResourceSummary
 *
 * <p>schema :
 * | ID(primary key) | Resource Type | Threshold | Value | Avg | Min | Max | Unit Type | Time Period |ID in HotNodeSummary(foreign key)
 * |      1          |    old gen    |    0.65   |  0.7  |     |     |     | percentage|    600      |          5
 */
public class HotResourceSummary extends GenericSummary {

  public static final String HOT_RESOURCE_SUMMARY_TABLE = HotResourceSummary.class.getSimpleName();
  private final ResourceType resourceType;
  private double threshold;
  private double value;
  private String unitType;
  private double avgValue;
  private double minValue;
  private double maxValue;
  private int timePeriod;

  public HotResourceSummary(ResourceType resourceType, double threshold,
      double value, String unitType, int timePeriod) {
    super();
    this.resourceType = resourceType;
    this.threshold = threshold;
    this.value = value;
    this.unitType = unitType;

    this.avgValue = Double.NaN;
    this.minValue = Double.NaN;
    this.maxValue = Double.NaN;
    this.timePeriod = timePeriod;
  }

  public void setValueDistribution(double minValue, double maxValue, double avgValue) {
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.avgValue = avgValue;
  }

  public ResourceType getResourceType() {
    return this.resourceType;
  }

  public String getResourceTypeName() {
    String resourceName = "unknown resource type";
    if (this.resourceType != null) {
      if (this.resourceType.getResourceTypeOneofCase() == ResourceTypeOneofCase.JVM) {
        resourceName = this.resourceType.getJVM().getValueDescriptor()
            .getOptions().getExtension(PANetworking.resourceTypeName);
      }
      else if (this.resourceType.getResourceTypeOneofCase() == ResourceTypeOneofCase.HARDWARE_RESOURCE_TYPE) {
        resourceName = this.resourceType.getHardwareResourceType().getValueDescriptor()
            .getOptions().getExtension(PANetworking.resourceTypeName);
      }
    }
    return resourceName;
  }

  public double getValue() {
    return this.value;
  }

  public String getUnitType() {
    return this.unitType;
  }

  public int getTimePeriod() {
    return this.timePeriod;
  }

  @Override
  public HotResourceSummaryMessage buildSummaryMessage() {
    final HotResourceSummaryMessage.Builder summaryMessageBuilder = HotResourceSummaryMessage
        .newBuilder();
    summaryMessageBuilder.setResourceType(this.resourceType);
    summaryMessageBuilder.setThreshold(this.threshold);
    summaryMessageBuilder.setValue(this.value);
    summaryMessageBuilder.setAvgValue(this.avgValue);
    summaryMessageBuilder.setMinValue(this.minValue);
    summaryMessageBuilder.setMaxValue(this.maxValue);
    summaryMessageBuilder.setUnitType(this.unitType);
    summaryMessageBuilder.setTimePeriod(this.timePeriod);
    for (GenericSummary nestedSummary : this.nestedSummaryList) {
      summaryMessageBuilder.getConsumersBuilder()
          .addConsumer(nestedSummary.buildSummaryMessage());
    }
    return summaryMessageBuilder.build();
  }

  @Override
  public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
    messageBuilder.setHotResourceSummary(this.buildSummaryMessage());
  }

  public static HotResourceSummary buildHotResourceSummaryFromMessage(
      HotResourceSummaryMessage message) {
    HotResourceSummary newSummary = new HotResourceSummary(message.getResourceType(), message.getThreshold(),
        message.getValue(), message.getUnitType(), message.getTimePeriod());
    newSummary
        .setValueDistribution(message.getMinValue(), message.getMaxValue(), message.getAvgValue());
    if (message.hasConsumers()) {
      for (int i = 0; i < message.getConsumers().getConsumerCount(); i++) {
        newSummary.addNestedSummaryList(TopConsumerSummary.buildTopConsumerSummaryFromMessage(
            message.getConsumers().getConsumer(i)));
      }
    }
    return newSummary;
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append(this.getResourceTypeName())
        .append(" ")
        .append(this.threshold)
        .append(" ")
        .append(this.value)
        .append(" ")
        .append(this.unitType)
        .append(" ")
        .append(this.nestedSummaryList)
        .toString();
  }

  @Override
  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schema = new ArrayList<>();
    schema.add(DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.RESOURCE_TYPE_COL_NAME), String.class));
    schema.add(DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.THRESHOLD_COL_NAME), Double.class));
    schema.add(DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.VALUE_COL_NAME), Double.class));
    schema.add(DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.AVG_VALUE_COL_NAME), Double.class));
    schema.add(DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.MIN_VALUE_COL_NAME), Double.class));
    schema.add(DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.MAX_VALUE_COL_NAME), Double.class));
    schema.add(DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.UNIT_TYPE_COL_NAME), String.class));
    schema.add(DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.TIME_PERIOD_COL_NAME), Integer.class));
    return schema;
  }

  @Override
  public List<Object> getSqlValue() {
    List<Object> value = new ArrayList<>();
    value.add(this.getResourceTypeName());
    value.add(Double.valueOf(this.threshold));
    value.add(Double.valueOf(this.value));
    value.add(Double.valueOf(this.avgValue));
    value.add(Double.valueOf(this.minValue));
    value.add(Double.valueOf(this.maxValue));
    value.add(this.unitType);
    value.add(Integer.valueOf(this.timePeriod));
    return value;
  }

  public static class SQL_SCHEMA_CONSTANTS {

    public static final String RESOURCE_TYPE_COL_NAME = "Resource Type";
    public static final String THRESHOLD_COL_NAME = "Threshold";
    public static final String VALUE_COL_NAME = "Value";
    public static final String AVG_VALUE_COL_NAME = "Avg Value";
    public static final String MIN_VALUE_COL_NAME = "Min Value";
    public static final String MAX_VALUE_COL_NAME = "Max Value";
    public static final String UNIT_TYPE_COL_NAME = "Unit Type";
    public static final String TIME_PERIOD_COL_NAME = "Time Period";
  }
}
