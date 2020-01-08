package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators;

public class SlidingWindowData {
  protected long timeStamp;
  protected double value;

  SlidingWindowData(long timeStamp, double value) {
    this.timeStamp = timeStamp;
    this.value = value;
  }

  public long getTimeStamp() {
    return this.timeStamp;
  }

  public double getValue() {
    return this.value;
  }
}



