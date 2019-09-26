package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;

public abstract class Symptom extends NonLeafNode {
  public Symptom(long evaluationIntervalSeconds) {
    super(0, evaluationIntervalSeconds);
  }
}
