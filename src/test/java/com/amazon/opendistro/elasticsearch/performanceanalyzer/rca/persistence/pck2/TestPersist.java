package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.pck2;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.ValueColumn;

public class TestPersist {
  @ValueColumn
  int x;

  public TestPersist() {
    this.x = 2;
  }

  public int getX() {
    return x;
  }
}
