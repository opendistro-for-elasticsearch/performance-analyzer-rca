package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.DecisionMakerConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.google.gson.JsonObject;

public class ReactionWheelTestUtil {

  public static String generateTestPayload(ModifyQueueCapacityAction action) {
    JsonObject payload = new JsonObject();
    payload.addProperty(DecisionMakerConsts.QUEUE_MAX_CAPACITY, action.getDesiredCapacity());
    return payload.toString();
  }
}
