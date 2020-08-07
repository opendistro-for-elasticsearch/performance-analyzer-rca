package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ActionListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.Plugin;
import com.amazon.searchservices.reactionwheel.controller.ControllerGrpc;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReactionWheelHandler extends Plugin implements ActionListener {
  public static final String NAME = "ReactionWheelHandler";
  private static final Logger LOG = LogManager.getLogger(ReactionWheelHandler.class);
  private final int PORT_NUM = 9100;
  private final String HOST = "localhost";
  private ManagedChannel channel;
  private ControllerGrpc.ControllerBlockingStub stub;
  private final Map<String, Function<Action, BatchStartControlRequest>> builderMap;

  public ReactionWheelHandler() {
    channel = buildInsecureChannel(HOST, PORT_NUM);
    stub = ControllerGrpc.newBlockingStub(channel);
    builderMap = initializeBuilderMap();
  }

  private Map<String, Function<Action, BatchStartControlRequest>> initializeBuilderMap() {
    Map<String, Function<Action, BatchStartControlRequest>> map = new HashMap<>();
    map.put(ModifyQueueCapacityAction.NAME,
        action -> ModifyQueueCapacityRequestBuilder.newBuilder((ModifyQueueCapacityAction) action).build());
    map.put(ModifyCacheMaxSizeAction.NAME,
            action -> ModifyCacheMaxSizeRequestBuilder.newBuilder((ModifyCacheMaxSizeAction) action).build());
    return Collections.unmodifiableMap(map);
  }

  // TODO: support https in the future ?
  private ManagedChannel buildInsecureChannel(final String remoteHost, final int port) {
    return ManagedChannelBuilder.forAddress(remoteHost, port).usePlaintext().build();
  }

  //TODO: we might want to add retry mechanism here in the future
  private void sendBatchRequest(BatchStartControlRequest request) {
    try {
      BatchStartControlResult result = stub.batchStartControl(request);
      if (result.getErrorsCount() > 0) {
        LOG.error("Reaction Wheel Client: error occurred while sending batch request");
      }
    }
    catch (StatusRuntimeException sre) {
      LOG.error("Reaction Wheel Client: exception occurred while sending batch request. Status: {}",
          sre.getStatus());
    }
  }

  public void shutdown() {
    channel.shutdown();
  }

  @Override
  public void actionPublished(Action action) {
    if (!action.isActionable()) {
      return;
    }
    if (!builderMap.containsKey(action.name())) {
      LOG.error("Reaction Wheel: unsupported action : {}", action.name());
      return;
    }
    BatchStartControlRequest request;
    try {
      request = builderMap.get(action.name()).apply(action);
    } catch (ClassCastException e) {
      LOG.error("Reaction Wheel: fail to cast action {}, trace : {}", action.name(), e.getStackTrace());
      return;
    }
    sendBatchRequest(request);
  }

  @Override
  public String name() {
    return NAME;
  }
}
