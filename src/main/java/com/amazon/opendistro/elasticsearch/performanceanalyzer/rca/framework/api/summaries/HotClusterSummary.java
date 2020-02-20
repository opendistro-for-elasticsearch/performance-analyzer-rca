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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.google.protobuf.GeneratedMessageV3;
import java.util.ArrayList;
import java.util.List;
import org.jooq.Field;
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

  @Override
  public String toString() {
    return this.numOfNodes + " " + this.numOfUnhealthyNodes + " " + this.nestedSummaryList;
  }

  @Override
  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schema = new ArrayList<>();
    schema.add(DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.NUM_OF_NODES_COL_NAME), Integer.class));
    schema.add(
        DSL.field(DSL.name(SQL_SCHEMA_CONSTANTS.NUM_OF_UNHEALTHY_NODES_COL_NAME), Integer.class));
    return schema;
  }

  @Override
  public List<Object> getSqlValue() {
    List<Object> value = new ArrayList<>();
    value.add(Integer.valueOf(this.numOfNodes));
    value.add(Integer.valueOf(this.numOfUnhealthyNodes));
    return value;
  }

  public static class SQL_SCHEMA_CONSTANTS {

    public static final String NUM_OF_NODES_COL_NAME = "Number of Nodes";
    public static final String NUM_OF_UNHEALTHY_NODES_COL_NAME = "Number of Unhealthy Nodes";
  }

}
