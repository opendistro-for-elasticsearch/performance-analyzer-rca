package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.tests.jvm.young_gen;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary.SQL_SCHEMA_CONSTANTS;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.framework.api.IValidator;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Assert;

public class YoungGenRcaValidator implements IValidator {
  long startTime;

  public YoungGenRcaValidator() {
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
      if (object.get("rca_name").getAsString().equals("HighHeapUsageYoungGenRca")) {
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

    JsonElement elem = object.get("HotResourceSummary");
    if (elem == null) {
      return false;
    }
    System.out.println("ENTERING JSON PARSING");
    JsonArray array = elem.getAsJsonArray();

    Assert.assertEquals(1, array.size());
    Assert.assertEquals(ResourceEnum.OLD_GEN.toString(),
        array.get(0).getAsJsonObject().get(SQL_SCHEMA_CONSTANTS.RESOURCE_TYPE_COL_NAME).getAsString());
    Assert.assertEquals(MetricEnum.FULL_GC.toString(),
        array.get(0).getAsJsonObject().get(SQL_SCHEMA_CONSTANTS.RESOURCE_METRIC_COL_NAME).getAsString());
    System.out.println("MATCHED MY BABY");
    return true;
  }
}
