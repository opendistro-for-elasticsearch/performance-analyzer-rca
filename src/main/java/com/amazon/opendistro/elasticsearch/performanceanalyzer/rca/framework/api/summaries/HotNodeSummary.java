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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.JooqFieldValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
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
 * HotNodeSummary collects and aggregates hot resource summaries on each data node It contains info
 * such as nodeID and node ip address. It is created by hot node RCA.
 *
 * <p>This object is persisted in SQLite table
 * Table name : HotNodeSummary
 *
 * <p>schema :
 * | ID(primary key) |        Node ID         | Host IP Address | ID in HotClusterSummary(foreign key)
 * |      1          | EIsMtfSdStSEisU6-x23Gw |   172.29.0.2    |          5
 */
public class HotNodeSummary extends GenericSummary {

  public static final String HOT_NODE_SUMMARY_TABLE = HotNodeSummary.class.getSimpleName();
  private static final Logger LOG = LogManager.getLogger(HotNodeSummary.class);
  private final String nodeID;
  private final String hostAddress;
  private final List<HotShardSummary> hotShardSummaryList;

  public HotNodeSummary(String nodeID, String hostAddress) {
    super();
    this.nodeID = nodeID;
    this.hostAddress = hostAddress;
    this.hotShardSummaryList = new ArrayList<>();
  }

  public HotNodeSummary(String nodeID, String hostAddress, final List<HotShardSummary> hotShardSummaryList) {
    super();
    this.nodeID = nodeID;
    this.hostAddress = hostAddress;
    this.hotShardSummaryList = hotShardSummaryList;
  }

  public String getNodeID() {
    return this.nodeID;
  }

  public String getHostAddress() {
    return this.hostAddress;
  }

  public List<HotShardSummary> getHotShardSummaryList() {
    return hotShardSummaryList;
  }


  @Override
  public HotNodeSummaryMessage buildSummaryMessage() {
    final HotNodeSummaryMessage.Builder summaryMessageBuilder = HotNodeSummaryMessage.newBuilder();
    summaryMessageBuilder.setNodeID(this.nodeID);
    summaryMessageBuilder.setHostAddress(this.hostAddress);
    for (GenericSummary nestedSummary : getNestedSummaryList()) {
      summaryMessageBuilder.getHotResourceSummaryListBuilder()
          .addHotResourceSummary(nestedSummary.buildSummaryMessage());
    }

    this.hotShardSummaryList.stream()
            .forEach(nestedHotShardSummary -> summaryMessageBuilder.getHotShardSummaryListBuilder()
                    .addHotShardSummary(nestedHotShardSummary.buildSummaryMessage()));
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

    if (message.hasHotShardSummaryList()
            && message.getHotShardSummaryList().getHotShardSummaryCount() > 0) {
      for (int i = 0; i < message.getHotShardSummaryList().getHotShardSummaryCount(); i++) {
        newSummary.addNestedSummaryList(HotShardSummary.buildHotShardSummaryFromMessage(
                message.getHotShardSummaryList().getHotShardSummary(i)));
      }
    }
    return newSummary;
  }

  @Override
  public String toString() {
    return this.nodeID + " " + this.hostAddress + " " + this.nestedSummaryList + " " + this.hotShardSummaryList;
  }

  @Override
  public String getTableName() {
    return HotNodeSummary.HOT_NODE_SUMMARY_TABLE;
  }

  @Override
  public List<SummaryBuilder<? extends GenericSummary>> getNestedSummaryBuilder() {
    return Collections.unmodifiableList(Collections.singletonList(
        new SummaryBuilder<>(HotResourceSummary.HOT_RESOURCE_SUMMARY_TABLE,
            HotResourceSummary::buildSummary)));
  }

  @Override
  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schema = new ArrayList<>();
    schema.add(NodeSummaryField.NODE_ID_FIELD.getField());
    schema.add(NodeSummaryField.HOST_IP_ADDRESS_FIELD.getField());
    return schema;
  }

  @Override
  public List<Object> getSqlValue() {
    List<Object> value = new ArrayList<>();
    value.add(this.nodeID);
    value.add(this.hostAddress);
    return value;
  }

  /**
   * Convert this summary object to JsonElement
   * @return JsonElement
   */
  @Override
  public JsonElement toJson() {
    JsonObject summaryObj = new JsonObject();
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME, this.nodeID);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME, this.hostAddress);
    if (!getNestedSummaryList().isEmpty()) {
      String tableName = getNestedSummaryList().get(0).getTableName();
      summaryObj.add(tableName, this.nestedSummaryListToJson());
    }

    this.hotShardSummaryList.forEach(
        summary -> {
          summaryObj.add(summary.getTableName(), summary.toJson());
        }
    );
    return summaryObj;
  }

  public static class SQL_SCHEMA_CONSTANTS {

    public static final String NODE_ID_COL_NAME = "node_id";
    public static final String HOST_IP_ADDRESS_COL_NAME = "host_address";
  }

  /**
   * Cluster summary SQL fields
   */
  public enum NodeSummaryField implements JooqFieldValue {
    NODE_ID_FIELD(SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME, String.class),
    HOST_IP_ADDRESS_FIELD(SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME,
        String.class);

    private String name;
    private Class<?> clazz;

    NodeSummaryField(final String name, Class<?> clazz) {
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
  public static HotNodeSummary buildSummary(Record record) {
    HotNodeSummary summary = null;
    try {
      String nodeId = record.get(NodeSummaryField.NODE_ID_FIELD.getField(), String.class);
      String ipAddress = record.get(NodeSummaryField.HOST_IP_ADDRESS_FIELD.getField(), String.class);
      summary = new HotNodeSummary(nodeId, ipAddress);
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
