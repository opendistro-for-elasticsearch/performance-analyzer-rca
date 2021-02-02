package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvm.young_gen.validator;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rest.QueryActionRequestHandler.ACTION_SET_JSON_NAME;
import static org.junit.Assert.assertNotEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.HeapSizeIncreaseAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.JvmGenAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction.SQL_SCHEMA_CONSTANTS;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class YoungGenNonBreachingValidator implements IValidator {
  @Override
  public boolean checkJsonResp(JsonElement response) {
    JsonArray array = response.getAsJsonObject().get(ACTION_SET_JSON_NAME).getAsJsonArray();
    // It could well be the case that no RCA has been triggered so far, and thus no action exists.
    // This is a valid outcome.
    if (array.size() == 0) {
      return true;
    }
    return checkPersistedAction(array);
  }

  private boolean checkPersistedAction(JsonArray array) {
    for (int i = 0; i < array.size(); i++) {
      JsonObject object = array.get(i).getAsJsonObject();
      //validate no heapSizeIncreaseAction is emitted
      assertNotEquals(HeapSizeIncreaseAction.NAME, object.get(SQL_SCHEMA_CONSTANTS.ACTION_COL_NAME).getAsString());

      //validate no young gen action is emitted
      assertNotEquals(JvmGenAction.NAME, object.get(SQL_SCHEMA_CONSTANTS.ACTION_COL_NAME).getAsString());
    }
    return true;
  }
}
