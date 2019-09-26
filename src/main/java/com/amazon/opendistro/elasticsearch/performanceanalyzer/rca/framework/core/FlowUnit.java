package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import java.util.List;
import java.util.Map;

/** This is the basic unit of transfer between the nodes. */
public class FlowUnit {
  private long timeStamp;

  // The first row is the name of the columns.
  // The values are second row and onwards.
  private List<List<String>> data;
  private Map<String, String> contextMap;
  private boolean empty = true;

  public FlowUnit(long timeStamp, List<List<String>> data, Map<String, String> contextMap) {
    this.timeStamp = timeStamp;
    this.data = data;
    this.contextMap = contextMap;
    empty = false;
  }

  // Creates an empty flow unit.
  public FlowUnit(long timeStamp) {
    this.timeStamp = timeStamp;
    this.data = null;
    this.contextMap = null;
    empty = false;
  }

  public static FlowUnit generic() {
    return new FlowUnit(System.currentTimeMillis());
  }

  public List<List<String>> getData() {
    return data;
  }

  public Map<String, String> getContextMap() {
    return contextMap;
  }

  public String getContextString() {
    return contextMap.toString();
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public boolean isEmpty() {
    return empty;
  }

  @Override
  public String toString() {
    return String.format("%d: %s :: context %s", timeStamp, data, contextMap);
  }
}
