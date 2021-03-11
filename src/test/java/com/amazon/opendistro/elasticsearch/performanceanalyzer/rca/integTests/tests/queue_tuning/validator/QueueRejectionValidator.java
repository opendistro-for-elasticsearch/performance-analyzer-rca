/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.queue_tuning.validator;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary.SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary.SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary.SQL_SCHEMA_CONSTANTS.RESOURCE_TYPE_COL_NAME;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.util.JsonParserUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Assert;

public class QueueRejectionValidator implements IValidator {
  long startTime;

  public QueueRejectionValidator() {
    startTime = System.currentTimeMillis();
  }

  /**
   * {"rca_name":"QueueRejectionClusterRca",
   * "timestamp":1596557050522,
   * "state":"unhealthy",
   * "HotClusterSummary":[
   * {"number_of_nodes":1,"number_of_unhealthy_nodes":1}
   * ]}
   */
  @Override
  public boolean checkJsonResp(JsonElement response) {
    JsonArray array = response.getAsJsonObject().get("data").getAsJsonArray();
    if (array.size() == 0) {
      return false;
    }

    for (int i = 0; i < array.size(); i++) {
      JsonObject object = array.get(i).getAsJsonObject();
      if (object.get("rca_name").getAsString().equals(QueueRejectionClusterRca.RCA_TABLE_NAME)) {
        return checkClusterRca(object);
      }
    }
    return false;
  }

  /**
   * {"rca_name":"QueueRejectionClusterRca",
   *  "timestamp":1597167456322,
   *  "state":"unhealthy",
   *  "HotClusterSummary":[{"number_of_nodes":1,"number_of_unhealthy_nodes":1}]
   * }
   */
  boolean checkClusterRca(JsonObject rcaObject) {
    if (!"unhealthy".equals(rcaObject.get("state").getAsString())) {
      return false;
    }
    Assert.assertEquals(1,
        JsonParserUtil.getSummaryJsonSize(rcaObject, HotClusterSummary.HOT_CLUSTER_SUMMARY_TABLE));
    JsonObject clusterSummaryJson =
        JsonParserUtil.getSummaryJson(rcaObject, HotClusterSummary.HOT_CLUSTER_SUMMARY_TABLE, 0);
    Assert.assertNotNull(clusterSummaryJson);
    Assert.assertEquals(1, clusterSummaryJson.get("number_of_unhealthy_nodes").getAsInt());

    Assert.assertEquals(1,
        JsonParserUtil.getSummaryJsonSize(clusterSummaryJson, HotNodeSummary.HOT_NODE_SUMMARY_TABLE));
    JsonObject nodeSummaryJson =
        JsonParserUtil.getSummaryJson(clusterSummaryJson, HotNodeSummary.HOT_NODE_SUMMARY_TABLE, 0);
    Assert.assertNotNull(nodeSummaryJson);
    Assert.assertEquals("DATA_0", nodeSummaryJson.get(NODE_ID_COL_NAME).getAsString());
    Assert.assertEquals("127.0.0.1", nodeSummaryJson.get(HOST_IP_ADDRESS_COL_NAME).getAsString());

    Assert.assertEquals(1,
        JsonParserUtil.getSummaryJsonSize(nodeSummaryJson, HotResourceSummary.HOT_RESOURCE_SUMMARY_TABLE));
    JsonObject resourceSummaryJson =
        JsonParserUtil.getSummaryJson(nodeSummaryJson, HotResourceSummary.HOT_RESOURCE_SUMMARY_TABLE, 0);
    Assert.assertNotNull(resourceSummaryJson);
    Assert.assertEquals("write threadpool", resourceSummaryJson.get(RESOURCE_TYPE_COL_NAME).getAsString());
    return true;
  }
}
