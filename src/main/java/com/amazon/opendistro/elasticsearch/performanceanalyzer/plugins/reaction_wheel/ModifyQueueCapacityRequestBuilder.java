package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.DecisionMakerConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel.ReactionWheelUtil.ControlType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest;
import com.google.gson.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ModifyQueueCapacityRequestBuilder {
  private static final Logger LOG = LogManager.getLogger(ModifyQueueCapacityRequestBuilder.class);
  private ModifyQueueCapacityAction action;

  private ModifyQueueCapacityRequestBuilder(ModifyQueueCapacityAction action) {
    this.action = action;
  }

  public static ModifyQueueCapacityRequestBuilder newBuilder(ModifyQueueCapacityAction action) {
    return new ModifyQueueCapacityRequestBuilder(action);
  }

  public BatchStartControlRequest build() {
    ControlType controlType = buildControlType();
    if (controlType == null) {
        LOG.error("Reaction Wheel: {} can not be tuned by action {}",
            action.getThreadPool().toString(), action.name());
        return null;
    }
    BatchStartControlRequest.Builder builder = BatchStartControlRequest.newBuilder();
    for (NodeKey targetNode : action.impactedNodes()) {
      ReactionWheel.Target reactionWheelTarget = ReactionWheelUtil.buildTarget(targetNode);
      ReactionWheel.Control reactionWheelControl = ReactionWheelUtil
          .buildControl(controlType, payload());
      ReactionWheel.Action reactionWheelAction = ReactionWheelUtil
          .buildAction(reactionWheelTarget, reactionWheelControl);
      builder.addActions(reactionWheelAction);
    }
    if (builder.getActionsCount() == 0) {
      return null;
    }
    return builder.build();
  }

  private String payload() {
    JsonObject payload = new JsonObject();
    payload.addProperty(DecisionMakerConsts.QUEUE_MAX_CAPACITY, action.getDesiredCapacity());
    return payload.toString();
  }

  private ControlType buildControlType() {
    switch (action.getThreadPool()) {
      case WRITE_THREADPOOL:
        return ControlType.WRITE_QUEUE_TUNING;
      case SEARCH_THREADPOOL:
        return ControlType.SEARCH_QUEUE_TUNING;
      default:
        return null;
    }
  }
}
