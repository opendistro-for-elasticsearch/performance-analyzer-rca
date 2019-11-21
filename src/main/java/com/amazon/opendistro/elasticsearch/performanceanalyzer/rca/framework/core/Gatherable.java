package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

public interface Gatherable {
  GenericFlowUnit gather(Queryable queryable);
}
