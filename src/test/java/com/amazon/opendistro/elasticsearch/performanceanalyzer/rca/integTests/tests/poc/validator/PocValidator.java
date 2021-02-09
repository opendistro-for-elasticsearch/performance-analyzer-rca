/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.poc.validator;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.List;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.Assert;

// Validators are only initialized once to evaluate a test method.
public class PocValidator implements IValidator {
  long startTime;

  public PocValidator() {
    startTime = System.currentTimeMillis();
  }

  /**
   * {"rca_name":"ClusterRca",
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
      if (object.get("rca_name").getAsString().equals("ClusterRca")) {
        return checkClusterRca(object);
      }
    }
    return false;
  }

  /**
   * {"rca_name":"ClusterRca",
   *  "timestamp":1597167456322,
   *  "state":"unhealthy",
   *  "HotClusterSummary":[{"number_of_nodes":1,"number_of_unhealthy_nodes":1}]
   * }
   */
  boolean checkClusterRca(JsonObject object) {
    if (!"unhealthy".equals(object.get("state").getAsString())) {
      return false;
    }

    JsonElement elem = object.get("HotClusterSummary");
    if (elem == null) {
      return false;
    }
    JsonArray array = elem.getAsJsonArray();

    Assert.assertEquals(1, array.size());
    Assert.assertEquals(1, array.get(0).getAsJsonObject().get("number_of_unhealthy_nodes").getAsInt());

    return true;
  }
}
