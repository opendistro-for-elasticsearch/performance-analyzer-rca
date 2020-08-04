package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.Action;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.Control;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.Target;

public class ReactionWheelUtil {

  public static final String CONTROL_VERSION = "1.0";

  public enum ControlType {
    REQUEST_CACHE_TUNING(Constants.REQUEST_CACHE_TUNING_VALUE),
    FIELDDATA_CACHE_TUNING(Constants.FIELDDATA_CACHE_TUNING_VALUE),
    WRITE_QUEUE_TUNING(Constants.WRITE_QUEUE_TUNING_VALUE),
    SEARCH_QUEUE_TUNING(Constants.SEARCH_QUEUE_TUNING_VALUE);


    private final String value;

    ControlType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String REQUEST_CACHE_TUNING_VALUE = "RequestCacheTuning";
      public static final String FIELDDATA_CACHE_TUNING_VALUE = "FieldDataCacheTuning";
      public static final String WRITE_QUEUE_TUNING_VALUE = "WriteQueueTuning";
      public static final String SEARCH_QUEUE_TUNING_VALUE = "SearchQueueTuning";
    }
  }

  public enum TargetType {
    NODE(Constants.NODE_VALUE),
    CLUSTER(Constants.CLUSTER_VALUE);


    private final String value;

    TargetType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String NODE_VALUE = "NODE";
      public static final String CLUSTER_VALUE = "CLUSTER";
    }
  }

  public static Target buildTarget(NodeKey nodeKey) {
    return Target.newBuilder()
        .setType(TargetType.NODE.toString())
        .setId(nodeKey.getNodeId().toString())
        .setIp(nodeKey.getHostAddress().toString()).build();
  }

  public static Control buildControl(ControlType controlType, String payload) {
    return Control.newBuilder()
        .setType(controlType.toString())
        .setVersion(CONTROL_VERSION)
        .setParams(payload).build();
  }

  public static Action buildAction(Target target, Control control) {
    return Action.newBuilder()
        .setTarget(target)
        .setControl(control).build();
  }

}
