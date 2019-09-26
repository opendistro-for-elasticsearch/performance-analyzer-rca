package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;

public abstract class Rca extends NonLeafNode {

  public Rca(long evaluationIntervalMins) {
    super(0, evaluationIntervalMins);
  }
}
