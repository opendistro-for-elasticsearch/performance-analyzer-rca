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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.NodeSummaryResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.RcaResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.ResourceSummaryResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jooq.Record;


public class RcaResponseUtil {

  /**
   * This function return RcaResponse object for given list of rca records.
   *
   * @param rcaName Name of the rca
   * @param recordList List of rca records which contains the node and resource level summary for the rca
   * @param tableNames All the valid tables present in database
   * @return  RcaResponse object for the input list of rca records
   */
  public static RcaResponse getRcaResponse(String rcaName, List<Record> recordList, Set<String> tableNames) {
    RcaResponse rcaResponse = null;
    Map<String, NodeSummaryResponse> nodeSummaryResponseMap = new HashMap<>();
    for (Record record : recordList) {
      if (rcaResponse == null) {
        rcaResponse = getRcaResponseForRecord(rcaName, record, tableNames);
      }
      if (tableNames.contains(HOT_NODE_SUMMARY_TABLE)) {
        String nodeId = record.get(HotNodeSummary.SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME, String.class);
        if (nodeId != null) {
          NodeSummaryResponse nodeSummaryResponse = nodeSummaryResponseMap.getOrDefault(nodeId,
                  getNodeSummary(nodeId, record));
          if (!nodeSummaryResponseMap.containsKey(nodeId)) {
            nodeSummaryResponseMap.put(nodeId, nodeSummaryResponse);
            rcaResponse.addSummary(nodeSummaryResponse);
          }
          if (tableNames.contains(HOT_RESOURCE_SUMMARY_TABLE)) {
            nodeSummaryResponse.addResource(getResourceSummaryResponse(record));
          }
        }
      }
    }
    return rcaResponse;
  }

  private static ResourceSummaryResponse getResourceSummaryResponse(Record record) {
    String resourceName = record.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.RESOURCE_TYPE_COL_NAME, String.class);
    String unitType = record.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.UNIT_TYPE_COL_NAME, String.class);
    Double threshold = record.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.THRESHOLD_COL_NAME, Double.class);
    Double average = record.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.AVG_VALUE_COL_NAME, Double.class);
    Double actual = record.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.VALUE_COL_NAME, Double.class);
    Double maximum = record.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.MAX_VALUE_COL_NAME, Double.class);
    Double minimum = record.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.MIN_VALUE_COL_NAME, Double.class);

    return new ResourceSummaryResponse(resourceName, unitType, threshold, actual, average, minimum, maximum);
  }

  private static NodeSummaryResponse getNodeSummary(String nodeId, Record record) {
    String ipAddress = record.get(HotNodeSummary.SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME, String.class);
    return new NodeSummaryResponse(nodeId, ipAddress);
  }

  private static RcaResponse getRcaResponseForRecord(String rcaName, Record record, Set<String> tableNames) {
    String timestamp = record.get(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME, String.class);
    String state = record.get(ResourceContext.SQL_SCHEMA_CONSTANTS.STATE_COL_NAME, String.class);

    if (!tableNames.contains(HOT_CLUSTER_SUMMARY_TABLE)) {
      return new RcaResponse(rcaName, state, timestamp);
    }

    Integer numOfNodes = record.get(HotClusterSummary.SQL_SCHEMA_CONSTANTS.NUM_OF_NODES_COL_NAME, Integer.class);
    Integer numOfUnhealthyNodes = record.get(HotClusterSummary.SQL_SCHEMA_CONSTANTS.NUM_OF_UNHEALTHY_NODES_COL_NAME, Integer.class);

    return new RcaResponse(rcaName, state, numOfNodes, numOfUnhealthyNodes, timestamp);
  }
}
