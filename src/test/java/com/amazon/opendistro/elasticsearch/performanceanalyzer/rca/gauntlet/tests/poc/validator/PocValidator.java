package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.tests.poc.validator;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.framework.api.IValidator;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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
