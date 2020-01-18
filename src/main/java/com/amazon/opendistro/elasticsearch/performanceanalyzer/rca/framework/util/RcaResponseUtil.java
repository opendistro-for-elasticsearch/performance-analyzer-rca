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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.HOT_CLUSTER_SUMMARY_TABLE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.HOT_NODE_SUMMARY_TABLE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.HOT_RESOURCE_SUMMARY_TABLE;

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

  private static final String NULL = "null";

  public static String convertRcaRecordListIntoJson(String rcaName, List<Record> recordList, Set<String> tableNames) {
    com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.RcaResponse rcaResponse = null;
    Map<String, NodeSummaryResponse> nodeSummaryResponseMap = new HashMap<>();
    for (Record record : recordList) {
      if (rcaResponse == null) {
        rcaResponse = getRcaResponse(rcaName, record, tableNames);
      }
      if (tableNames.contains(HOT_NODE_SUMMARY_TABLE)) {
        String nodeId = getStringFieldValue(record, HotNodeSummary.SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME);
        NodeSummaryResponse nodeSummaryResponse = nodeSummaryResponseMap.getOrDefault(nodeId,
                getNodeSummary(nodeId, record));
        if (!nodeSummaryResponseMap.containsKey(nodeId)) {
          nodeSummaryResponseMap.put(nodeId, nodeSummaryResponse);
          rcaResponse.addNodeSummaryResponse(nodeSummaryResponse);
        }
        if (tableNames.contains(HOT_RESOURCE_SUMMARY_TABLE)) {
          nodeSummaryResponse.addResource(getResourceSummaryResponse(record));
        }
      }
    }
    return rcaResponse == null ? "{}" : rcaResponse.toString();
  }

  private static ResourceSummaryResponse getResourceSummaryResponse(Record record) {
    String resourceName = getStringFieldValue(record,
            HotResourceSummary.SQL_SCHEMA_CONSTANTS.RESOURCE_TYPE_COL_NAME);
    String unitType = getStringFieldValue(record,
            HotResourceSummary.SQL_SCHEMA_CONSTANTS.UNIT_TYPE_COL_NAME);
    Double threshold = getDoubleFieldValue(record,
            HotResourceSummary.SQL_SCHEMA_CONSTANTS.THRESHOLD_COL_NAME);
    Double average = getDoubleFieldValue(record,
            HotResourceSummary.SQL_SCHEMA_CONSTANTS.AVG_VALUE_COL_NAME);
    Double actual = getDoubleFieldValue(record,
            HotResourceSummary.SQL_SCHEMA_CONSTANTS.VALUE_COL_NAME);
    Double maximum = getDoubleFieldValue(record,
            HotResourceSummary.SQL_SCHEMA_CONSTANTS.MAX_VALUE_COL_NAME);
    Double minimum = getDoubleFieldValue(record,
            HotResourceSummary.SQL_SCHEMA_CONSTANTS.MIN_VALUE_COL_NAME);

    return new ResourceSummaryResponse(resourceName, unitType, threshold, actual, average, minimum, maximum);
  }

  private static String getStringFieldValue(Record record, String fieldName) {
    Object value = record.getValue(fieldName);
    return value == null ? NULL : value.toString();
  }

  private static Double getDoubleFieldValue(Record record, String fieldName) {
    String value = getStringFieldValue(record, fieldName);
    return value.equals(NULL) ? null : Double.parseDouble(value);
  }

  private static Integer getIntegerFieldValue(Record record, String fieldName) {
    String value = getStringFieldValue(record, fieldName);
    return value.equals(NULL) ? null : Integer.parseInt(value);
  }

  private static NodeSummaryResponse getNodeSummary(String nodeId, Record record) {
    String ipAddress = record.getValue(HotNodeSummary.SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME).toString();
    return new NodeSummaryResponse(nodeId, ipAddress);
  }

  private static RcaResponse getRcaResponse(String rcaName, Record record, Set<String> tableNames) {
    String timestamp = getStringFieldValue(record,
            ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME);
    String state = getStringFieldValue(record,
            ResourceContext.SQL_SCHEMA_CONSTANTS.STATE_COL_NAME);

    if (!tableNames.contains(HOT_CLUSTER_SUMMARY_TABLE)) {
      return new RcaResponse(rcaName, state, timestamp);
    }

    Integer numOfNodes = getIntegerFieldValue(record,
            HotClusterSummary.SQL_SCHEMA_CONSTANTS.NUM_OF_NODES_COL_NAME);
    Integer numOfUnhealthyNodes = getIntegerFieldValue(record,
            HotClusterSummary.SQL_SCHEMA_CONSTANTS.NUM_OF_UNHEALTHY_NODES_COL_NAME);

    return new RcaResponse(rcaName, state, numOfNodes, numOfUnhealthyNodes, timestamp);
  }
}
