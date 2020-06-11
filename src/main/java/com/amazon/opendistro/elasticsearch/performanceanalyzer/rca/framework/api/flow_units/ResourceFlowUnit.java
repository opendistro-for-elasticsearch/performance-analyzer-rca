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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.temperature.CompactNodeTemperatureFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.JooqFieldValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * ResourceFlowUnit is the flowunit type that is emitted by RCA vertex.
 * It is persisted in the FlowUnit SQLite table
 *
 * <p>The SQL table name  : FlowUnit
 *
 * <p>SQL Schema :
 * | ID(primary key) | Timestamp |      RCA_Name        | state
 * |      1          |  151000   |  HighHeapYoungGenRca | healthy
 */
public class ResourceFlowUnit<T extends GenericSummary> extends GenericFlowUnit {

  private static final Logger LOG = LogManager.getLogger(ResourceFlowUnit.class);
  public static final String RCA_TABLE_NAME = "RCA";
  private ResourceContext resourceContext = null;
  private T summary = null;
  // whether summary needs to be persisted as well when persisting this flowunit
  private boolean persistSummary = false;

  public ResourceFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public ResourceFlowUnit(long timeStamp, ResourceContext context,
      T summary, boolean persistSummary) {
    super(timeStamp);
    this.resourceContext = context;
    this.summary = summary;
    this.empty = false;
    this.persistSummary = persistSummary;
  }

  public ResourceFlowUnit(long timeStamp, ResourceContext context,
      T summary) {
    this(timeStamp, context, summary, false);
  }

  //Call generic() only if you want to generate a empty flowunit
  public static ResourceFlowUnit<? extends GenericSummary> generic() {
    return new ResourceFlowUnit<>(System.currentTimeMillis());
  }

  public ResourceContext getResourceContext() {
    return this.resourceContext;
  }

  public void setResourceContext(ResourceContext context) {
    this.resourceContext = context;
  }

  public boolean hasResourceSummary() {
    return this.summary != null;
  }

  public GenericSummary getPersistableSummary() {
    return this.summary;
  }

  public T getSummary() {
    return summary;
  }

  public void setSummary(T summary) {
    this.summary = summary;
  }

  public void setPersistSummary(boolean persistSummary) {
    this.persistSummary = persistSummary;
  }

  public boolean isSummaryPersistable() {
    return this.persistSummary;
  }

  @Override
  public FlowUnitMessage buildFlowUnitMessage(final String graphNode, final String esNode) {
    final FlowUnitMessage.Builder messageBuilder = FlowUnitMessage.newBuilder();
    messageBuilder.setGraphNode(graphNode);
    messageBuilder.setEsNode(esNode);
    messageBuilder.setTimeStamp(System.currentTimeMillis());
    if (resourceContext != null) {
          messageBuilder.setResourceContext(resourceContext.buildContextMessage());
    }

    if (summary != null) {
      summary.buildSummaryMessageAndAddToFlowUnit(messageBuilder);
    }
    return messageBuilder.build();
  }

  /**
   * parse the "oneof" section in protocol buffer call the corresponding object build function for
   * each summary type
   */
  @SuppressWarnings("unchecked")
  public static <T extends GenericSummary> ResourceFlowUnit<T> buildFlowUnitFromWrapper(final FlowUnitMessage message) {
    //if the flowunit is empty. empty flowunit does not have context
    if (message.hasResourceContext()) {
      ResourceContext newContext = ResourceContext
          .buildResourceContextFromMessage(message.getResourceContext());
      T newSummary = null;
      try {
        switch (message.getSummaryOneofCase()) {
          case HOTRESOURCESUMMARY: {
            newSummary = (T) HotResourceSummary
                .buildHotResourceSummaryFromMessage(message.getHotResourceSummary());
            break;
          }
          case HOTNODESUMMARY: {
            newSummary = (T) HotNodeSummary
                .buildHotNodeSummaryFromMessage(message.getHotNodeSummary());
            break;
          }
        }
      } catch (Exception e) {
        // we are not supposed to run into this unless we specified wrong summary template
        // for this function. Make sure the summary type passed in as template are consistent
        // between serialization and de-serializing.
        LOG.error("RCA: casting to wrong summary type when de-serializing this flowunit");
      }
      return new ResourceFlowUnit<>(message.getTimeStamp(), newContext, newSummary);
    } else {
      //empty flowunit;
      //TODO: we might not want to send empty flowunit across network.
      return new ResourceFlowUnit<>(message.getTimeStamp());
    }
  }

  /**
   * Read the SQL schema of the FlowUnit table that persists this FlowUnit.
   * @return list of Field object.
   */
  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schema = new ArrayList<>();
    if (!this.isEmpty()) {
      schema.add(ResourceFlowUnitFieldValue.TIMESTAMP_FIELD.getField());
      schema.add(ResourceFlowUnitFieldValue.RCA_NAME_FILELD.getField());
      schema.add(ResourceFlowUnitFieldValue.STATE_NAME_FILELD.getField());
    }
    return schema;
  }

  /**
   * Read the values of this FlowUnit as a SQL row.
   * @param rcaName The name of the RCA vertex to be inserted into SQL
   * @return List of Objects
   */
  public List<Object> getSqlValue(String rcaName) {
    List<Object> value = new ArrayList<>();
    if (!this.isEmpty()) {
      value.add(getTimeStamp());
      value.add(rcaName);
      value.addAll(this.getResourceContext().getSqlValue());
    }
    return value;
  }

  @Override
  public String toString() {
    return this.getTimeStamp() + ": " + resourceContext + " :: " + summary;
  }

  public enum ResourceFlowUnitFieldValue implements JooqFieldValue {
    TIMESTAMP_FIELD(SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME, Long.class),
    RCA_NAME_FILELD(SQL_SCHEMA_CONSTANTS.RCA_COL_NAME, String.class),
    STATE_NAME_FILELD(SQL_SCHEMA_CONSTANTS.STATE_COL_NAME, String.class);

    private String name;
    private Class<?> clazz;
    ResourceFlowUnitFieldValue(final String name, Class<?> clazz) {
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

  public static class SQL_SCHEMA_CONSTANTS {

    public static final String TIMESTAMP_COL_NAME = "timestamp";
    public static final String RCA_COL_NAME = "rca_name";
    public static final String STATE_COL_NAME = "state";

  }
}
