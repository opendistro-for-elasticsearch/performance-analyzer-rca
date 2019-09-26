package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.FlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import java.util.Map;

public class OperationArgWrapper {
  private final Node node;
  private final Queryable queryable;
  private final Persistable persistable;
  private final WireHopper wireHopper;
  Map<Class, FlowUnit> upstreamDependencyMap;

  OperationArgWrapper(
      Node node,
      Queryable queryable,
      Persistable persistable,
      WireHopper wireHopper,
      Map<Class, FlowUnit> upstreamDependencyMap) {
    this.node = node;
    this.queryable = queryable;
    this.persistable = persistable;
    this.wireHopper = wireHopper;
    this.upstreamDependencyMap = upstreamDependencyMap;
  }

  public Node getNode() {
    return node;
  }

  public Queryable getQueryable() {
    return queryable;
  }

  Persistable getPersistable() {
    return persistable;
  }

  Map<Class, FlowUnit> getUpstreamDependencyMap() {
    return upstreamDependencyMap;
  }

  public WireHopper getWireHopper() {
    return wireHopper;
  }
}
