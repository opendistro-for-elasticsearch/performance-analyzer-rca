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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * HotResourceSummary contains information such as the name of the hot resource, the current value
 * threshold, etc. It also contains the top K consumers of this particular resource. It is created
 * by some RCAs who work directly on some type of resource(JVM, CPU etc.)
 */
public class HotResourceSummary extends GenericSummary {

  private static final Logger LOG = LogManager.getLogger(HotResourceSummary.class);
  private final Enum<? extends Resources.ResourceType> resourceType;
  private List<String> consumers;
  private double threshold;
  private double value;
  private String unitType;
  private double avgValue;
  private double minValue;
  private double maxValue;
  private int timePeriod;

  public HotResourceSummary(Enum<? extends Resources.ResourceType> resourceType, double threshold,
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

  public void addConsumers(List<String> consumers) {
    this.consumers = consumers;
  }

  public void setValueDistribution(double minValue, double maxValue, double avgValue) {
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.avgValue = avgValue;
  }

  public Enum<? extends Resources.ResourceType> getResourceType() {
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
    summaryMessageBuilder
        .setResourceType(resourceType.getClass().getName() + "." + resourceType.name());
    summaryMessageBuilder.setThreshold(this.threshold);
    summaryMessageBuilder.setValue(this.value);
    summaryMessageBuilder.setAvgValue(this.avgValue);
    summaryMessageBuilder.setMinValue(this.minValue);
    summaryMessageBuilder.setMaxValue(this.maxValue);
    summaryMessageBuilder.setUnitType(this.unitType);
    summaryMessageBuilder.setTimePeriod(this.timePeriod);
    return summaryMessageBuilder.build();
  }

  @Override
  public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
    messageBuilder.setHotResourceSummary(this.buildSummaryMessage());
  }

  public static HotResourceSummary buildHotResourceSummaryFromMessage(
      HotResourceSummaryMessage message) {
    String resourceTypeClassName = message.getResourceType();
    //cast to Enum using reflection. find the last "." and separate the class name into two parts.
    //e.g. com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources$ResourceType.HEAP
    //will be split to  "com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources$ResourceType"
    //and "HEAP"
    String[] nameSplit = resourceTypeClassName.split("\\.(?=[^\\.]+$)");
    Enum<? extends Resources.ResourceType> resourcetypeEnum = null;
    if (nameSplit.length == 2) {
      String enumClassName = nameSplit[0];
      String enumName = nameSplit[1];
      try {
        @SuppressWarnings("unchecked")
        Class<Enum> cl = (Class<Enum>) Class.forName(enumClassName);
        resourcetypeEnum = Enum.valueOf(cl, enumName);
      } catch (ClassNotFoundException e) {
        LOG.error("Exception occurs when casting to enum !");
      }
    }
    if (resourcetypeEnum == null) {
      LOG.error("Fails to cast back to Enum, the Enum class name received is : {}",
          resourceTypeClassName);
      return null;
    }
    HotResourceSummary newSummary = new HotResourceSummary(resourcetypeEnum, message.getThreshold(),
        message.getValue(), message.getUnitType(), message.getTimePeriod());
    newSummary
        .setValueDistribution(message.getMinValue(), message.getMaxValue(), message.getAvgValue());
    if (message.hasConsumers() && message.getConsumers().getConsumerCount() > 0) {
      newSummary.addConsumers(IntStream.range(0, message.getConsumers().getConsumerCount())
          .mapToObj(i -> message.getConsumers().getConsumer(i))
          .collect(Collectors.toList()));
    }
    return newSummary;
  }

  @Override
  public String toString() {
    return this.resourceType.toString() + " " + this.consumers + " " + this.threshold + " "
        + this.value + " " + this.unitType;
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
    value.add(this.resourceType.toString());
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
