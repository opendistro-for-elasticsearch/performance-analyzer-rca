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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.JooqFieldValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.exception.DataTypeException;
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
 * | ID           | Resource Type | Threshold | Value | Avg | Min | Max | Unit Type | Time_Period_Seconds |ID in HotNodeSummary
 *  (primary key)                                                                                           (foreign key)
 * |      1       |    old gen    |    0.65   |  0.7  |     |     |     | percentage|          600        |          5
 */
public class HotResourceSummary extends GenericSummary {

  public static final String HOT_RESOURCE_SUMMARY_TABLE = HotResourceSummary.class.getSimpleName();
  private static final Logger LOG = LogManager.getLogger(HotResourceSummary.class);
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
        .append(ResourceTypeUtil.getResourceTypeName(this.resourceType))
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
  public String getTableName() {
    return HotResourceSummary.HOT_RESOURCE_SUMMARY_TABLE;
  }

  @Override
  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schema = new ArrayList<>();
    schema.add(ResourceSummaryField.RESOURCE_TYPE_FIELD.getField());
    schema.add(ResourceSummaryField.THRESHOLD_FILELD.getField());
    schema.add(ResourceSummaryField.VALUE_FILELD.getField());
    schema.add(ResourceSummaryField.AVG_VALUE_FILELD.getField());
    schema.add(ResourceSummaryField.MIN_VALUE_FILELD.getField());
    schema.add(ResourceSummaryField.MAX_VALUE_FILELD.getField());
    schema.add(ResourceSummaryField.UNIT_TYPE_FILELD.getField());
    schema.add(ResourceSummaryField.TIME_PERIOD_FILELD.getField());
    return schema;
  }

  @Override
  public List<Object> getSqlValue() {
    List<Object> value = new ArrayList<>();
    value.add(ResourceTypeUtil.getResourceTypeName(this.resourceType));
    value.add(Double.valueOf(this.threshold));
    value.add(Double.valueOf(this.value));
    value.add(Double.valueOf(this.avgValue));
    value.add(Double.valueOf(this.minValue));
    value.add(Double.valueOf(this.maxValue));
    value.add(this.unitType);
    value.add(Integer.valueOf(this.timePeriod));
    return value;
  }

  /**
   * Convert this summary object to JsonElement
   * @return JsonElement
   */
  @Override
  public JsonElement toJson() {
    JsonObject summaryObj = new JsonObject();
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.RESOURCE_TYPE_COL_NAME,
        ResourceTypeUtil.getResourceTypeName(this.resourceType));
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.THRESHOLD_COL_NAME, this.threshold);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.VALUE_COL_NAME, this.value);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.AVG_VALUE_COL_NAME, this.avgValue);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.MIN_VALUE_COL_NAME, this.minValue);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.MAX_VALUE_COL_NAME, this.maxValue);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.UNIT_TYPE_COL_NAME, this.unitType);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.TIME_PERIOD_COL_NAME, this.timePeriod);
    this.nestedSummaryList.forEach(
        summary -> {
          summaryObj.add(summary.getTableName(), summary.toJson());
        }
    );
    return summaryObj;
  }

  public static class SQL_SCHEMA_CONSTANTS {

    public static final String RESOURCE_TYPE_COL_NAME = "resource_type";
    public static final String THRESHOLD_COL_NAME = "threshold";
    public static final String VALUE_COL_NAME = "value";
    public static final String AVG_VALUE_COL_NAME = "avg";
    public static final String MIN_VALUE_COL_NAME = "min";
    public static final String MAX_VALUE_COL_NAME = "max";
    public static final String UNIT_TYPE_COL_NAME = "unit_type";
    public static final String TIME_PERIOD_COL_NAME = "time_period_seconds";
  }

  /**
   * Cluster summary SQL fields
   */
  public enum ResourceSummaryField implements JooqFieldValue {
    RESOURCE_TYPE_FIELD(SQL_SCHEMA_CONSTANTS.RESOURCE_TYPE_COL_NAME, String.class),
    THRESHOLD_FILELD(SQL_SCHEMA_CONSTANTS.THRESHOLD_COL_NAME, Double.class),
    VALUE_FILELD(SQL_SCHEMA_CONSTANTS.VALUE_COL_NAME, Double.class),
    AVG_VALUE_FILELD(SQL_SCHEMA_CONSTANTS.AVG_VALUE_COL_NAME, Double.class),
    MIN_VALUE_FILELD(SQL_SCHEMA_CONSTANTS.MIN_VALUE_COL_NAME, Double.class),
    MAX_VALUE_FILELD(SQL_SCHEMA_CONSTANTS.MAX_VALUE_COL_NAME, Double.class),
    UNIT_TYPE_FILELD(SQL_SCHEMA_CONSTANTS.UNIT_TYPE_COL_NAME, String.class),
    TIME_PERIOD_FILELD(SQL_SCHEMA_CONSTANTS.TIME_PERIOD_COL_NAME, Integer.class);


    private String name;
    private Class<?> clazz;

    ResourceSummaryField(final String name, Class<?> clazz) {
      this.name = name;
      this.clazz = clazz;
    }

    @Override
    public Field<?> getField() {
      return DSL.field(DSL.name(this.name), this.clazz);
    }

    @Override
    public String getName() {
      return this.name;
    }
  }

  /**
   * Re-generate the node summary object from SQL query result.
   * @param record SQLite record
   * @return node summary object
   */
  public static HotResourceSummary buildSummary(Record record) {
    HotResourceSummary summary = null;
    try {
      String resourceTypeName = record.get(ResourceSummaryField.RESOURCE_TYPE_FIELD.getField(), String.class);
      Double threshold = record.get(ResourceSummaryField.THRESHOLD_FILELD.getField(), Double.class);
      Double value = record.get(ResourceSummaryField.VALUE_FILELD.getField(), Double.class);
      Double avgValue = record.get(ResourceSummaryField.AVG_VALUE_FILELD.getField(), Double.class);
      Double minValue = record.get(ResourceSummaryField.MIN_VALUE_FILELD.getField(), Double.class);
      Double maxValue = record.get(ResourceSummaryField.MAX_VALUE_FILELD.getField(), Double.class);
      String unitType = record.get(ResourceSummaryField.UNIT_TYPE_FILELD.getField(), String.class);
      Integer timePeriod = record.get(ResourceSummaryField.TIME_PERIOD_FILELD.getField(), Integer.class);
      summary = new HotResourceSummary(ResourceTypeUtil.buildResourceType(resourceTypeName),
          threshold, value, unitType, timePeriod);
      if ((!Double.isNaN(avgValue))
          && (!Double.isNaN(minValue))
          && (!Double.isNaN(maxValue))) {
        summary.setValueDistribution(minValue, maxValue, avgValue);
      }
    }
    catch (IllegalArgumentException ie) {
      LOG.error("Some field is not found in record, cause : {}", ie.getMessage());
    }
    catch (DataTypeException de) {
      LOG.error("Fails to convert data type");
    }
    return summary;
  }
}
