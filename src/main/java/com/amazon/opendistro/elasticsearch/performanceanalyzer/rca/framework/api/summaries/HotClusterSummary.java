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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.JooqFieldValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.GeneratedMessageV3;
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
 * HotClusterSummary is a cluster level summary. It collects and aggregates node summaries from each
 * data nodes and additional info will be added by master. This type of summary is created by
 * cluster level RCAs whcih only run on elected master.
 *
 * <p>This object is persisted in SQLite table
 * Table name : HotClusterSummary
 *
 * <p>schema :
 * | ID(primary key) | Number of nodes | unhealthy nodes | ID in FlowUnit(foreign key)
 * |      1          |      5          |        1        |          5
 */
public class HotClusterSummary extends GenericSummary {

  public static final String HOT_CLUSTER_SUMMARY_TABLE = HotClusterSummary.class.getSimpleName();
  private static final Logger LOG = LogManager.getLogger(HotClusterSummary.class);
  private int numOfNodes;
  private int numOfUnhealthyNodes;

  public HotClusterSummary(int numOfNodes, int numOfUnhealthyNodes) {
    super();
    this.numOfNodes = numOfNodes;
    this.numOfUnhealthyNodes = numOfUnhealthyNodes;
  }

  /**
   * HotClusterSummary is supposed to be created on elected master node only. and we do not expect
   * it to be sent via gRPC. Return null in all the methods below. and we should not define the gRPC
   * message wrapper for this summary class in protocol buf.
   */
  @Override
  public GeneratedMessageV3 buildSummaryMessage() {
    return null;
  }

  @Override
  public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
  }

  public int getNumOfNodes() {
    return numOfNodes;
  }

  public int getNumOfUnhealthyNodes() {
    return numOfUnhealthyNodes;
  }

  @Override
  public String toString() {
    return this.numOfNodes + " " + this.numOfUnhealthyNodes + " " + this.nestedSummaryList;
  }

  @Override
  public String getTableName() {
    return HotClusterSummary.HOT_CLUSTER_SUMMARY_TABLE;
  }

  @Override
  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schema = new ArrayList<>();
    schema.add(ClusterSummaryField.NUM_OF_NODES_FIELD.getField());
    schema.add(ClusterSummaryField.NUM_OF_UNHEALTHY_NODES_FIELD.getField());
    return schema;
  }

  @Override
  public List<Object> getSqlValue() {
    List<Object> value = new ArrayList<>();
    value.add(Integer.valueOf(this.numOfNodes));
    value.add(Integer.valueOf(this.numOfUnhealthyNodes));
    return value;
  }

  /**
   * Convert this summary object to JsonElement
   * @return JsonElement
   */
  @Override
  public JsonElement toJson() {
    JsonObject summaryObj = new JsonObject();
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.NUM_OF_NODES_COL_NAME, this.numOfNodes);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.NUM_OF_UNHEALTHY_NODES_COL_NAME, this.numOfUnhealthyNodes);
    if (!this.nestedSummaryList.isEmpty()) {
      String tableName = this.nestedSummaryList.get(0).getTableName();
      summaryObj.add(tableName, this.nestedSummaryListToJson());
    }
    return summaryObj;
  }

  public static class SQL_SCHEMA_CONSTANTS {

    public static final String NUM_OF_NODES_COL_NAME = "number_of_nodes";
    public static final String NUM_OF_UNHEALTHY_NODES_COL_NAME = "number_of_unhealthy_nodes";
  }

  /**
   * Cluster summary SQL fields
   */
  public enum ClusterSummaryField implements JooqFieldValue {
    NUM_OF_NODES_FIELD(SQL_SCHEMA_CONSTANTS.NUM_OF_NODES_COL_NAME, Integer.class),
    NUM_OF_UNHEALTHY_NODES_FIELD(SQL_SCHEMA_CONSTANTS.NUM_OF_UNHEALTHY_NODES_COL_NAME,
        Integer.class);

    private String name;
    private Class<?> clazz;

    ClusterSummaryField(final String name, Class<?> clazz) {
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
   * parse SQL query result and fill the result into summary obj.
   * @param record SQLite record
   * @return whether parsing is successful or not
   */
  @Nullable
  public static GenericSummary buildSummary(Record record) {
    GenericSummary summary = null;
    try {
      Integer numOfNodes = record.get(ClusterSummaryField.NUM_OF_NODES_FIELD.getField(), Integer.class);
      Integer numOfUnhealthyNodes = record.get(ClusterSummaryField.NUM_OF_UNHEALTHY_NODES_FIELD.getField(), Integer.class);
      summary = new HotClusterSummary(numOfNodes, numOfUnhealthyNodes);
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
