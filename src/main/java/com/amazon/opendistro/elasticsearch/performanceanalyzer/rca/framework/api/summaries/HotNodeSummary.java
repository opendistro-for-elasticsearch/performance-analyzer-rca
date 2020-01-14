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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.HotNodeSummaryMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import java.util.ArrayList;
import java.util.List;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * HotNodeSummary collects and aggregates hot resource summaries on each data node It contains info
 * such as nodeID and node ip address. It is created by hot node RCA.
 */
public class HotNodeSummary extends GenericSummary {

  public final String nodeID;
  public final String hostAddress;

  public HotNodeSummary(String nodeID, String hostAddress) {
    super();
    this.nodeID = nodeID;
    this.hostAddress = hostAddress;
  }

  @Override
  public HotNodeSummaryMessage buildSummaryMessage() {
    final HotNodeSummaryMessage.Builder summaryMessageBuilder = HotNodeSummaryMessage.newBuilder();
    summaryMessageBuilder.setNodeID(this.nodeID);
    summaryMessageBuilder.setHostAddress(this.hostAddress);
    for (GenericSummary nestedSummary : this.nestedSummaryList) {
      summaryMessageBuilder.getHotResourceSummaryListBuilder()
          .addHotResourceSummary(nestedSummary.buildSummaryMessage());
    }
    return summaryMessageBuilder.build();
  }

  @Override
  public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
    messageBuilder.setHotNodeSummary(this.buildSummaryMessage());
  }

  public static HotNodeSummary buildHotNodeSummaryFromMessage(HotNodeSummaryMessage message) {
    HotNodeSummary newSummary = new HotNodeSummary(message.getNodeID(), message.getHostAddress());
    if (message.hasHotResourceSummaryList()
        && message.getHotResourceSummaryList().getHotResourceSummaryCount() > 0) {
      for (int i = 0; i < message.getHotResourceSummaryList().getHotResourceSummaryCount(); i++) {
        newSummary.addNestedSummaryList(HotResourceSummary.buildHotResourceSummaryFromMessage(
            message.getHotResourceSummaryList().getHotResourceSummary(i)));
      }
    }
    return newSummary;
  }

  @Override
  public String toString() {
    return this.nodeID + " " + this.hostAddress + " " + this.nestedSummaryList;
  }

  @Override
  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schema = new ArrayList<>();
    schema.add(DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME), String.class));
    schema.add(DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME), String.class));
    return schema;
  }

  @Override
  public List<Object> getSqlValue() {
    List<Object> value = new ArrayList<>();
    value.add(this.nodeID);
    value.add(this.hostAddress);
    return value;
  }

  public static class SQL_SCHEMA_CONSTANTS {

    public static final String NODE_ID_COL_NAME = "Node ID";
    public static final String HOST_IP_ADDRESS_COL_NAME = "Host IP";
  }
}
