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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;

import java.util.ArrayList;
import java.util.List;
import org.jooq.Field;
import org.jooq.impl.DSL;

public class ResourceFlowUnit extends GenericFlowUnit {

  private ResourceContext resourceContext = null;
  private GenericSummary resourceSummary = null;
  // whether summary needs to be persisted as well when persisting this flowunit
  private boolean persistSummary = false;

  public ResourceFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public <S extends GenericSummary> ResourceFlowUnit(long timeStamp, ResourceContext context,
      S resourceSummary, boolean persistSummary) {
    super(timeStamp);
    this.resourceContext = context;
    this.resourceSummary = resourceSummary;
    this.empty = false;
    this.persistSummary = persistSummary;
  }

  public <S extends GenericSummary> ResourceFlowUnit(long timeStamp, ResourceContext context,
      S resourceSummary) {
    this(timeStamp, context, resourceSummary, false);
  }

  //Call generic() only if you want to generate a empty flowunit
  public static ResourceFlowUnit generic() {
    return new ResourceFlowUnit(System.currentTimeMillis());
  }

  public ResourceContext getResourceContext() {
    return this.resourceContext;
  }

  public boolean hasResourceSummary() {
    return this.resourceSummary != null;
  }

  public GenericSummary getResourceSummary() {
    return this.resourceSummary;
  }

  public <S extends GenericSummary> void setResourceSummary(S summary) {
    this.resourceSummary = summary;
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

    if (resourceSummary != null) {
      resourceSummary.buildSummaryMessageAndAddToFlowUnit(messageBuilder);
    }
    return messageBuilder.build();
  }

  /**
   * parse the "oneof" section in protocol buffer call the corresponding object build function for
   * each summary type
   */
  public static ResourceFlowUnit buildFlowUnitFromWrapper(final FlowUnitMessage message) {
    //if the flowunit is empty. empty flowunit does not have context
    if (message.hasResourceContext()) {
      ResourceContext newContext = ResourceContext
          .buildResourceContextFromMessage(message.getResourceContext());
      GenericSummary newSummary = null;
      if (message.getSummaryOneofCase().getNumber()
          == FlowUnitMessage.SummaryOneofCase.HOTRESOURCESUMMARY.getNumber()
          && message.hasHotResourceSummary()) {
        newSummary = HotResourceSummary
            .buildHotResourceSummaryFromMessage(message.getHotResourceSummary());
      } else if (message.getSummaryOneofCase().getNumber()
          == FlowUnitMessage.SummaryOneofCase.HOTNODESUMMARY.getNumber()
          && message.hasHotNodeSummary()) {
        newSummary = HotNodeSummary.buildHotNodeSummaryFromMessage(message.getHotNodeSummary());
      }
      return new ResourceFlowUnit(message.getTimeStamp(), newContext, newSummary);
    } else {
      //empty flowunit;
      //TODO: we might not want to send empty flowunit across network.
      return new ResourceFlowUnit(message.getTimeStamp());
    }
  }

  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schema = new ArrayList<>();
    if (!this.isEmpty()) {
      schema.add(DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME), String.class));
      schema.addAll(this.getResourceContext().getSqlSchema());
    }
    return schema;
  }

  public List<Object> getSqlValue() {
    List<Object> value = new ArrayList<>();
    if (!this.isEmpty()) {
      value.add(String.valueOf(this.getTimeStamp()));
      value.addAll(this.getResourceContext().getSqlValue());
    }
    return value;
  }

  @Override
  public String toString() {
    return this.getTimeStamp() + ": " + resourceContext + " :: " + resourceSummary;
  }


  public static class SQL_SCHEMA_CONSTANTS {

    public static final String TIMESTAMP_COL_NAME = "TimeStamp";
  }
}
