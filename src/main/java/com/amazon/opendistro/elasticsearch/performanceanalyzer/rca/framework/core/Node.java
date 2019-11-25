/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Node {
  private List<Node> downStreams;
  protected List<Node> upStreams;
  private int level;

  private int graphId;

  protected long evaluationIntervalSeconds;

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

  void setLevel(int level) {
    this.level = level;
  }

  int getLevel() {
    return level;
  }

  public void setGraphId(int graphId) {
    this.graphId = graphId;
  }

  public int getGraphId() {
    return graphId;
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

  public abstract void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args);

  public abstract void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args);

  public abstract void setGernericFlowUnitList();

  public abstract List<? extends GenericFlowUnit> fetchFlowUnitList();

  @Override
  public String toString() {
    return name();
  }
}
