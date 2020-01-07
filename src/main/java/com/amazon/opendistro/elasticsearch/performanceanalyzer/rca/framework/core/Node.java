/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

public abstract class Node<T extends GenericFlowUnit> {
  // TODO: Make bounds explicit

  /**
   * List of downstream nodes for this node.
   */
  private List<Node<?>> downStreams;

  /**
   * List of upstream nodes for this node.
   */
  protected List<Node<?>> upStreams;

  /**
   * The depth level of the current node in the overall graph
   */
  private int level;

  /**
   * The graphId for the node.
   */
  private int graphId;

  /**
   * Time in seconds which represents the periodicity with which the scheduler executes this node.
   */
  protected long evaluationIntervalSeconds;

  /**
   * List of flow units produced by this node obtained either from evaluating it locally or
   * obtaining them from other nodes in the cluster.
   */
  protected List<T> flowUnits;

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

  void addDownstream(Node<?> downStreamNode) {
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

  List<Node<?>> getDownStreams() {
    if (downStreams == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(downStreams);
  }

  public List<Node<?>> getUpstreams() {
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

  public abstract void persistFlowUnit(FlowUnitOperationArgWrapper args);

  public abstract void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args);

  public void setEmptyFlowUnitList() {
    flowUnits = Collections.emptyList();
  }

  @Override
  public String toString() {
    return name();
  }

  public List<T> getFlowUnits() {
    return flowUnits;
  }

  public void setFlowUnits(List<T> flowUnits) {
    this.flowUnits = flowUnits;
  }
}
