package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ActionTestUtil {
  private Map<ResourceEnum, Action> actionMap;
  private Map<String, Function<Action, ResourceEnum>> resourceEnumMap;

  public ActionTestUtil(List<Action> actions) {
    actionMap = new HashMap<>();
    resourceEnumMap = generateResourceEnumMap();
    for (Action action : actions) {
      if (!resourceEnumMap.containsKey(action.name())) {
        throw new IllegalArgumentException();
      }
      ResourceEnum resourceEnum = resourceEnumMap.get(action.name()).apply(action);
      actionMap.put(resourceEnum, action);
    }
  }

  private Map<String, Function<Action, ResourceEnum>> generateResourceEnumMap() {
    Map<String, Function<Action, ResourceEnum>> map = new HashMap<>();
    map.put(ModifyQueueCapacityAction.NAME, action -> ((ModifyQueueCapacityAction)action).getThreadPool());
    map.put(ModifyCacheMaxSizeAction.NAME, action -> ((ModifyCacheMaxSizeAction)action).getCacheType());
    return Collections.unmodifiableMap(map);
  }

  public <T extends Action> T readAction(ResourceEnum resourceEnum, Class<T> clazz) throws ClassCastException {
    if (!actionMap.containsKey(resourceEnum)) {
      return null;
    }
    Action action = actionMap.get(resourceEnum);
    return clazz.cast(action);
  }
}
