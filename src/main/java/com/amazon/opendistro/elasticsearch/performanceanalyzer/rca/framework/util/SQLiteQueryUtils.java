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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary.HOT_CLUSTER_SUMMARY_TABLE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary.HOT_NODE_SUMMARY_TABLE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary.HOT_RESOURCE_SUMMARY_TABLE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.TopConsumerSummary.TOP_CONSUMER_SUMMARY_TABLE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit.ResourceFlowUnitFieldValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ClusterDimensionalSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ClusterDimensionalSummary.ZoneSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ClusterTemperatureSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.NodeLevelDimensionalSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ShardProfileSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.ClusterTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.NodeTemperatureRca;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SelectJoinStep;
import org.jooq.impl.DSL;

/**
 * A utility class to query cluster, node and resource level summary for a rca
 */
public class SQLiteQueryUtils {
  private static final Map<String, String> nestedTableMap;
  private static final Map<String, String> temperatureProfileNestedSummaryMap;
  private static final Set<String> clusterLevelRCA;
  private static final Set<String> temperatureProfileRCASet;

  // to map table => its nested table
  // e.g. HotClusterSummary => HotNodeSummary
  static {
    Map<String, String> tableMap = new HashMap<>();
    tableMap.put(ClusterTemperatureSummary.TABLE_NAME, ClusterDimensionalSummary.TABLE_NAME);

    tableMap.put(ResourceFlowUnit.RCA_TABLE_NAME, HOT_CLUSTER_SUMMARY_TABLE);
    tableMap.put(HOT_CLUSTER_SUMMARY_TABLE, HOT_NODE_SUMMARY_TABLE);
    tableMap.put(HOT_NODE_SUMMARY_TABLE, HOT_RESOURCE_SUMMARY_TABLE);
    tableMap.put(HOT_RESOURCE_SUMMARY_TABLE, TOP_CONSUMER_SUMMARY_TABLE);
    nestedTableMap = Collections.unmodifiableMap(tableMap);
  }

  static {
    Map<String, String> temperatureSummaryMap = new HashMap<>();
    temperatureSummaryMap.put(ResourceFlowUnit.RCA_TABLE_NAME,
        NodeLevelDimensionalSummary.SUMMARY_TABLE_NAME);
    temperatureSummaryMap.put(NodeLevelDimensionalSummary.SUMMARY_TABLE_NAME,
        NodeLevelDimensionalSummary.ZONE_SUMMARY_TABLE_NAME);
    temperatureSummaryMap.put(NodeLevelDimensionalSummary.ZONE_SUMMARY_TABLE_NAME,
        ShardProfileSummary.SUMMARY_TABLE_NAME);

    temperatureProfileNestedSummaryMap = Collections.unmodifiableMap(temperatureSummaryMap);
  }

  // RCAs that can be queried by RCA API
  // currently we can only query from the cluster level RCAs
  static {
    Set<String> rcaSet = new HashSet<>();

    rcaSet.add(ClusterTemperatureRca.TABLE_NAME);
    rcaSet.add(HighHeapUsageClusterRca.RCA_TABLE_NAME);
    rcaSet.add(HotNodeClusterRca.RCA_TABLE_NAME);
    rcaSet.add(QueueRejectionClusterRca.RCA_TABLE_NAME);
    clusterLevelRCA = Collections.unmodifiableSet(rcaSet);
  }

  // Temperature profile RCAs that can be queried by the RCA API.
  static {
    Set<String> tempProfileRcaSet = new HashSet<>();

    tempProfileRcaSet.add(NodeTemperatureRca.TABLE_NAME);
    tempProfileRcaSet.add(ClusterTemperatureRca.TABLE_NAME);
    temperatureProfileRCASet = Collections.unmodifiableSet(tempProfileRcaSet);
  }

  /**
   * get a mapping between table => its nested table
   * e.g. HotClusterSummary => HotNodeSummary
   * @return map table => its nested table
   */
  public static Map<String, String> getNestedTableMap() {
    return nestedTableMap;
  }

  /**
   * get a list of all the RCAs that are supported by RCA API.
   * @return list of supported RCAs
   */
  public static List<String> getClusterLevelRca() {
    return ImmutableList.copyOf(clusterLevelRCA);
  }

  /**
   * check if the rca is a cluster level rca
   * @param rca the name of rca
   * @return if it is a cluster level rca
   */
  public static boolean isClusterLevelRca(String rca) {
    if (rca == null) {
      return false;
    }
    return clusterLevelRCA.contains(rca);
  }

  /**
   * This function build SQL query to fetch a rca from RCA table
   *
   * @param ctx DSLContext
   * @param rca The rca that will be queried
   * @return jooq query object
   */
  public static SelectJoinStep<Record> buildRcaQuery(final DSLContext ctx,
                                              final String rca) {
    SelectJoinStep<Record> rcaQuery = ctx.select().from(ResourceFlowUnit.RCA_TABLE_NAME);
    rcaQuery.where(
        DSL.field(ResourceFlowUnitFieldValue.RCA_NAME_FILELD.getName(), String.class).equal(rca)
    )
        .orderBy(ResourceFlowUnitFieldValue.TIMESTAMP_FIELD.getField().desc());
    return rcaQuery;
  }

  /**
   * This function build SQL query to fetch summary from a summay table
   * @param ctx DSLContext
   * @param tableName the summary table to query from
   * @param foreignKey the foreign used in where clause
   * @param foreignKeyField the foriegn key field in this table
   * @return jooq query object
   */
  public static SelectJoinStep<Record> buildSummaryQuery(final DSLContext ctx,
      final String tableName,
      final int foreignKey,
      final Field<Integer> foreignKeyField) {
    SelectJoinStep<Record> summaryQuery = ctx.select().from(tableName);
    summaryQuery.where(foreignKeyField.equal(foreignKey));
    return summaryQuery;
  }

  /**
   * generate the name of primary key field in each table
   * @param tableName table name
   * @return primary key field name
   */
  public static String getPrimaryKeyColumnName(String tableName) {
    return tableName + "_ID";
  }

  public static List<String> getTemperatureProfileRcas() {
    return ImmutableList.copyOf(temperatureProfileRCASet);
  }

  public static boolean isTemperatureProfileRca(String rca) {
    if (rca == null) {
      return false;
    }

    return temperatureProfileRCASet.contains(rca);
  }
}


