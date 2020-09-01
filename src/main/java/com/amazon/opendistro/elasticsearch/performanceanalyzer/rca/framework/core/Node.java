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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;

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
   * List of flow units produced by this node obtained from evaluating other nodes in the cluster.
   */
  protected List<T> flowUnits;

  /**
   * Flow unit produced by this vertex obtained from evaluating it locally.
   */
  protected T localFlowUnit;
  /**
   * These are matched against the tags in the rca.conf, to determine if a node is to executed at a
   * location.
   */
  private Map<String, String> tags;

  /**
   * A view of the instanceDetails that the RCAs can have access to.
   */
  private AppContext appContext;

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

  @VisibleForTesting
  public void setEvaluationIntervalSeconds(long value) {
    evaluationIntervalSeconds = value;
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

  /**
   * This method specifies what needs to be done when the current node is muted for throwing
   * exceptions.
   */
  public abstract void handleNodeMuted();

  public void setEmptyFlowUnitList() {
    flowUnits = Collections.emptyList();
  }

  public void setEmptyLocalFlowUnit() {
    this.localFlowUnit = null;
  }

  @Override
  public String toString() {
    return name();
  }

  @NonNull
  public List<T> getFlowUnits() {
    List<T> allFlowUnits = flowUnits == null ? new ArrayList<>() : new ArrayList<>(flowUnits);

    if (localFlowUnit != null) {
      allFlowUnits.add(localFlowUnit);
    }

    return allFlowUnits;
  }

  public void setFlowUnits(List<T> flowUnits) {
    this.flowUnits = flowUnits;
  }

  public void setLocalFlowUnit(T localFlowUnit) {
    this.localFlowUnit = localFlowUnit;
  }

  /**
   * callback function to parse local rca.conf file and set RCA thresholds accordingly
   * The default callback function does nothing because we assume most of the RCA vertices
   * does not read threshold settings from external config
   * @param conf RcaConf object
   */
  public void readRcaConf(RcaConf conf) {
    return;
  }

  public void setAppContext(final AppContext appContext) {
    this.appContext = appContext;
  }

  protected AppContext getAppContext() {
    return this.appContext;
  }

  public InstanceDetails getInstanceDetails() {
    InstanceDetails ret = new InstanceDetails(AllMetrics.NodeRole.UNKNOWN);
    if (this.appContext != null) {
      ret = this.appContext.getMyInstanceDetails();
    }
    return ret;
  }

  public List<InstanceDetails> getAllClusterInstances() {
    List<InstanceDetails> ret = Collections.EMPTY_LIST;

    if (this.appContext != null) {
      ret = this.appContext.getAllClusterInstances();
    }
    return ret;
  }

  public List<InstanceDetails> getDataNodeInstances() {
    List<InstanceDetails> ret = Collections.EMPTY_LIST;
    if (this.appContext != null) {
      return this.appContext.getDataNodeInstances();
    }
    return ret;
  }
}
