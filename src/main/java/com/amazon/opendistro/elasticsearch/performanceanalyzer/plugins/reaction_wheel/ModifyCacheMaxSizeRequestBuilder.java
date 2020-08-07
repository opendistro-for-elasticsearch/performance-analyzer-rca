package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.DecisionMakerConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel.ReactionWheelUtil.ControlType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest;
import com.google.gson.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A {@link ModifyCacheMaxSizeRequestBuilder} whose requests are handled to reaction wheel.
 *
 * <p>This class defines request builder for {@link ModifyCacheMaxSizeAction}. This action is used to 
 * increase/decrease the cache max size.
 */
public class ModifyCacheMaxSizeRequestBuilder {
    private static final Logger LOG = LogManager.getLogger(ModifyCacheMaxSizeRequestBuilder.class);
    private ModifyCacheMaxSizeAction action;

    private ModifyCacheMaxSizeRequestBuilder(final ModifyCacheMaxSizeAction action) {
        this.action = action;
    }

    public static ModifyCacheMaxSizeRequestBuilder newBuilder(ModifyCacheMaxSizeAction action) {
        return new ModifyCacheMaxSizeRequestBuilder(action);
    }

    public BatchStartControlRequest build() {
        ControlType controlType = buildControlType();
        if (controlType == null) {
            LOG.error("Reaction Wheel: {} can not be tuned by action {}",
                    action.getCacheType().toString(), action.name());
            return null;
        }
        BatchStartControlRequest.Builder builder = BatchStartControlRequest.newBuilder();
        NodeKey targetNode = action.impactedNodes().get(0);
        ReactionWheel.Target reactionWheelTarget = ReactionWheelUtil.buildTarget(targetNode);
        ReactionWheel.Control reactionWheelControl = ReactionWheelUtil
                .buildControl(controlType, payload());
        ReactionWheel.Action reactionWheelAction = ReactionWheelUtil
                .buildAction(reactionWheelTarget, reactionWheelControl);
        builder.addActions(reactionWheelAction);
        if (builder.getActionsCount() == 0) {
            return null;
        }
        return builder.build();
    }

    private String payload() {
        JsonObject payload = new JsonObject();
        payload.addProperty(DecisionMakerConsts.CACHE_MAX_WEIGHT, action.getDesiredCacheMaxSizeInBytes());
        return payload.toString();
    }

    private ControlType buildControlType() {
        switch (action.getCacheType()) {
            case FIELD_DATA_CACHE:
                return ControlType.FIELDDATA_CACHE_TUNING;
            case SHARD_REQUEST_CACHE:
                return ControlType.REQUEST_CACHE_TUNING;
            default:
                return null;
        }
    }
}

