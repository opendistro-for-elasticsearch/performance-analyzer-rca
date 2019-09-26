package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

public interface Gatherable {
  FlowUnit gather(Queryable queryable);
}
