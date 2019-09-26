package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Node {
  protected List<Node> upStreams;
  protected long evaluationIntervalSeconds;
  private List<Node> downStreams;
  private int level;
  private int graphId;
  /**
   * These are matched against the tags in the rca.conf, to determine if a node is to executed at a
   * location.
   */
  private Map<String, String> tags;

  Node(int level, long evaluationIntervalSeconds) {
    this.downStreams = new ArrayList<>();
    this.level = level;
    this.evaluationIntervalSeconds = evaluationIntervalSeconds;
    this.tags = new HashMap<>();
  }

  void addDownstream(Node downStreamNode) {
    this.downStreams.add(downStreamNode);
  }

  int getLevel() {
    return level;
  }

  void setLevel(int level) {
    this.level = level;
  }

  public int getGraphId() {
    return graphId;
  }

  public void setGraphId(int graphId) {
    this.graphId = graphId;
  }

  public long getEvaluationIntervalSeconds() {
    return evaluationIntervalSeconds;
  }

  int getUpStreamNodesCount() {
    if (upStreams == null) {
      return 0;
    }
    return upStreams.size();
  }

  List<Node> getDownStreams() {
    if (downStreams == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(downStreams);
  }

  public List<Node> getUpstreams() {
    if (upStreams == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(upStreams);
  }

  public Map<String, String> getTags() {
    return Collections.unmodifiableMap(tags);
  }

  public void addTag(String key, String value) {
    tags.put(key, value);
  }

  public String name() {
    return getClass().getSimpleName();
  }

  @Override
  public String toString() {
    return name();
  }
}
