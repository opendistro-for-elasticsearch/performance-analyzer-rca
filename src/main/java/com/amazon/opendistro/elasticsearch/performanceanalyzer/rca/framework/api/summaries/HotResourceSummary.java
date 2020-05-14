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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
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
  private double avgValue;
  private double minValue;
  private double maxValue;
  private int timePeriod;
  private String metaData;

  public HotResourceSummary(ResourceType resourceType, double threshold,
      double value, int timePeriod) {
    super();
    this.resourceType = resourceType;
    this.threshold = threshold;
    this.value = value;

    this.avgValue = Double.NaN;
    this.minValue = Double.NaN;
    this.maxValue = Double.NaN;
    this.timePeriod = timePeriod;
    this.metaData = "";
  }

  public HotResourceSummary(ResourceType resourceType, double threshold,
                            double value, int timePeriod, String metaData) {
    super();
    this.resourceType = resourceType;
    this.threshold = threshold;
    this.value = value;

    this.avgValue = Double.NaN;
    this.minValue = Double.NaN;
    this.maxValue = Double.NaN;
    this.timePeriod = timePeriod;
    this.metaData = metaData;
  }

  public void setValueDistribution(double minValue, double maxValue, double avgValue) {
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.avgValue = avgValue;
  }

  /**
   * buildHotResourceSummaryFromMessage() requires each nestedSummaryList element to be of the
   * {@link com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.TopConsumerSummaryMessage} class, otherwise the
   * method will throw a {@link ClassCastException} upon invocation. We override addNestedSummaryList() s.t. user
   * error cannot cause an Exception.
   */
  @Override
  public void addNestedSummaryList(Collection<GenericSummary> nestedSummaryList) {
    for (GenericSummary summary: nestedSummaryList) {
      if (!(summary instanceof TopConsumerSummary)) {
        LOG.error("Attempted to add nested summary of unsupported type {} to HotResourceSummary",
                summary.getClass().getSimpleName());
        return;
      }
    }
    this.nestedSummaryList.addAll(nestedSummaryList);
  }

  @Override
  public void addNestedSummaryList(GenericSummary nestedSummary) {
    if (!(nestedSummary instanceof TopConsumerSummary)) {
      LOG.error("Attempted to add nested summary of unsupported type {} to HotResourceSummary",
              nestedSummary.getClass().getSimpleName());
      return;
    }
    this.nestedSummaryList.add(nestedSummary);
  }

  public ResourceType getResourceType() {
    return this.resourceType;
  }

  public double getValue() {
    return this.value;
  }

  public double getThreshold() {
    return this.threshold;
  }

  public int getTimePeriod() {
    return this.timePeriod;
  }

  public String getMetaData() {
    return this.metaData;
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
    summaryMessageBuilder.setTimePeriod(this.timePeriod);
    summaryMessageBuilder.setMetaData(this.metaData);
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

  public static HotResourceSummary buildHotResourceSummaryFromMessage(HotResourceSummaryMessage message) {
    HotResourceSummary newSummary = new HotResourceSummary(message.getResourceType(), message.getThreshold(),
        message.getValue(), message.getTimePeriod());
    newSummary.setValueDistribution(message.getMinValue(), message.getMaxValue(), message.getAvgValue());
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
        .append(ResourceTypeUtil.getResourceTypeUnit(this.resourceType))
        .append(" ")
        .append(this.nestedSummaryList)
        .append(" ")
        .append(this.metaData)
        .toString();
  }

  @Override
  public String getTableName() {
    return HotResourceSummary.HOT_RESOURCE_SUMMARY_TABLE;
  }

  @Override
  public List<Class<? extends GenericSummary>> getNestedSummaryClassType() {
    return Collections.unmodifiableList(Collections.singletonList(
        TopConsumerSummary.class));
  }

  @Override
  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schema = new ArrayList<>();
    schema.add(ResourceSummaryField.RESOURCE_TYPE_FIELD.getField());
    schema.add(ResourceSummaryField.THRESHOLD_FIELD.getField());
    schema.add(ResourceSummaryField.VALUE_FIELD.getField());
    schema.add(ResourceSummaryField.AVG_VALUE_FIELD.getField());
    schema.add(ResourceSummaryField.MIN_VALUE_FIELD.getField());
    schema.add(ResourceSummaryField.MAX_VALUE_FIELD.getField());
    schema.add(ResourceSummaryField.UNIT_TYPE_FIELD.getField());
    schema.add(ResourceSummaryField.TIME_PERIOD_FIELD.getField());
    schema.add(ResourceSummaryField.METADATA_FIELD.getField());
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
    value.add(ResourceTypeUtil.getResourceTypeUnit(this.resourceType));
    value.add(Integer.valueOf(this.timePeriod));
    value.add(metaData);
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
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.UNIT_TYPE_COL_NAME,
        ResourceTypeUtil.getResourceTypeUnit(this.resourceType));
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.TIME_PERIOD_COL_NAME, this.timePeriod);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.META_DATA_COL_NAME, this.metaData);
    if (!this.nestedSummaryList.isEmpty()) {
      String tableName = this.nestedSummaryList.get(0).getTableName();
      summaryObj.add(tableName, this.nestedSummaryListToJson());
    }
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
    public static final String META_DATA_COL_NAME = "meta_data";
  }

  /**
   * Cluster summary SQL fields
   */
  public enum ResourceSummaryField implements JooqFieldValue {
    RESOURCE_TYPE_FIELD(SQL_SCHEMA_CONSTANTS.RESOURCE_TYPE_COL_NAME, String.class),
    THRESHOLD_FIELD(SQL_SCHEMA_CONSTANTS.THRESHOLD_COL_NAME, Double.class),
    VALUE_FIELD(SQL_SCHEMA_CONSTANTS.VALUE_COL_NAME, Double.class),
    AVG_VALUE_FIELD(SQL_SCHEMA_CONSTANTS.AVG_VALUE_COL_NAME, Double.class),
    MIN_VALUE_FIELD(SQL_SCHEMA_CONSTANTS.MIN_VALUE_COL_NAME, Double.class),
    MAX_VALUE_FIELD(SQL_SCHEMA_CONSTANTS.MAX_VALUE_COL_NAME, Double.class),
    UNIT_TYPE_FIELD(SQL_SCHEMA_CONSTANTS.UNIT_TYPE_COL_NAME, String.class),
    TIME_PERIOD_FIELD(SQL_SCHEMA_CONSTANTS.TIME_PERIOD_COL_NAME, Integer.class),
    METADATA_FIELD(SQL_SCHEMA_CONSTANTS.META_DATA_COL_NAME, String.class);


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
  @Nullable
  public static HotResourceSummary buildSummary(Record record) {
    HotResourceSummary summary = null;
    try {
      String resourceTypeName = record.get(ResourceSummaryField.RESOURCE_TYPE_FIELD.getField(), String.class);
      Double threshold = record.get(ResourceSummaryField.THRESHOLD_FIELD.getField(), Double.class);
      Double value = record.get(ResourceSummaryField.VALUE_FIELD.getField(), Double.class);
      Double avgValue = record.get(ResourceSummaryField.AVG_VALUE_FIELD.getField(), Double.class);
      Double minValue = record.get(ResourceSummaryField.MIN_VALUE_FIELD.getField(), Double.class);
      Double maxValue = record.get(ResourceSummaryField.MAX_VALUE_FIELD.getField(), Double.class);
      Integer timePeriod = record.get(ResourceSummaryField.TIME_PERIOD_FIELD.getField(), Integer.class);
      String metaData = record.get(ResourceSummaryField.METADATA_FIELD.getField(), String.class);
      summary = new HotResourceSummary(ResourceTypeUtil.buildResourceType(resourceTypeName),
          threshold, value, timePeriod, metaData);
      // those three fields are optional. check before setting to the obj
      if (avgValue != null
          && minValue != null
          && maxValue != null) {
        summary.setValueDistribution(minValue, maxValue, avgValue);
      }
    }
    catch (IllegalArgumentException ie) {
      LOG.error("Some fields might not be found in record, cause : {}", ie.getMessage());
    }
    catch (DataTypeException de) {
      LOG.error("Fails to convert data type");
    }
    // we are very unlikely to catch this exception unless some fields are not persisted properly.
    catch (NullPointerException ne) {
      LOG.error("read null object from SQL, trace : {} ", ne.getStackTrace());
    }
    return summary;
  }
}
