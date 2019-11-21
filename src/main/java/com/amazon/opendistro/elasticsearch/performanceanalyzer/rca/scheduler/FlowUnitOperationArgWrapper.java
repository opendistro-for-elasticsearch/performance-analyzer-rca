package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;


import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.NetPersistor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;

public class FlowUnitOperationArgWrapper {
  private final Node node;
  private final Queryable queryable;
  private final Persistable persistable;
  private final WireHopper wireHopper;
  private final NetPersistor netPersistor;

  public Node getNode() {
    return node;
  }

  public Queryable getQueryable() {
    return queryable;
  }

  public Persistable getPersistable() {
    return persistable;
  }

  public WireHopper getWireHopper() {
    return wireHopper;
  }

  FlowUnitOperationArgWrapper(
      Node node, Queryable queryable, Persistable persistable, WireHopper wireHopper) {
    this.node = node;
    this.queryable = queryable;
    this.persistable = persistable;
    this.wireHopper = wireHopper;
    this.netPersistor = null;
  }
}
