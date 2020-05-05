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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.JooqFieldValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.exception.DataTypeException;
import org.jooq.impl.DSL;

/**
 * TopConsumerSummary contains the name and usage of a resource consumer.
 */
public class TopConsumerSummary extends GenericSummary {

  public static final String TOP_CONSUMER_SUMMARY_TABLE = TopConsumerSummary.class.getSimpleName();
  private static final Logger LOG = LogManager.getLogger(HotClusterSummary.class);
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
   * TopConsumerSummary is the lowest level summary in the nest summary hierarchy.
   * So it doesn't carry any nested summary list and thus we override this with an empty method.
   */
  @Override
  public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
  }

  public static TopConsumerSummary buildTopConsumerSummaryFromMessage(TopConsumerSummaryMessage message) {
    return new TopConsumerSummary(message.getName(), message.getValue());
  }

  @Override
  public String toString() {
    return this.name + " " + this.value;
  }

  @Override
  public String getTableName() {
    return TopConsumerSummary.TOP_CONSUMER_SUMMARY_TABLE;
  }

  @Override
  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schema = new ArrayList<>();
    schema.add(TopConsumerSummaryField.CONSUMER_NAME_FIELD.getField());
    schema.add(TopConsumerSummaryField.CONSUMER_VALUE_FIELD.getField());
    return schema;
  }

  @Override
  public List<Object> getSqlValue() {
    List<Object> value = new ArrayList<>();
    value.add(this.name);
    value.add(this.value);
    return value;
  }

  @Override
  public JsonElement toJson() {
    JsonObject summaryObj = new JsonObject();
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.CONSUMER_NAME_COL_NAME, this.name);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.CONSUMER_VALUE_COL_NAME, this.value);
    return summaryObj;
  }

  public static class SQL_SCHEMA_CONSTANTS {

    public static final String CONSUMER_NAME_COL_NAME = "name";
    public static final String CONSUMER_VALUE_COL_NAME = "value";
  }

  /**
   * top consumer summary SQL fields
   */
  public enum TopConsumerSummaryField implements JooqFieldValue {
    CONSUMER_NAME_FIELD(SQL_SCHEMA_CONSTANTS.CONSUMER_NAME_COL_NAME, String.class),
    CONSUMER_VALUE_FIELD(SQL_SCHEMA_CONSTANTS.CONSUMER_VALUE_COL_NAME, Double.class);

    private String name;
    private Class<?> clazz;

    TopConsumerSummaryField(final String name, Class<?> clazz) {
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
   * restore the top consumer object from SQL query result.
   * @param record SQLite record
   * @return top consumer object
   */
  @Nullable
  public static TopConsumerSummary buildSummary(Record record) {
    TopConsumerSummary summary = null;
    try {
      String name = record.get(TopConsumerSummaryField.CONSUMER_NAME_FIELD.getField(), String.class);
      Double value = record.get(TopConsumerSummaryField.CONSUMER_VALUE_FIELD.getField(), Double.class);
      if (name != null && value != null) {
        summary = new TopConsumerSummary(name, value);
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
