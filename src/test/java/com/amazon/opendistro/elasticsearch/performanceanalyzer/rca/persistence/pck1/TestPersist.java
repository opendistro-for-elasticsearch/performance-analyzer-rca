package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.pck1;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.ValueColumn;

public class TestPersist {
  @ValueColumn
  int x;

  public TestPersist() {
    this.x = 1;
  }

  public int getX() {
    return x;
  }

  public void setX(int x) {
    x = x;
  }
}
