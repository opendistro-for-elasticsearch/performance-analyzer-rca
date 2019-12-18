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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

/*
 * This is the container for all the Analysis dependency graphs. Usually this is the first class that is created
 * and
 */

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AnalysisGraph {

  private static final Logger LOG = LogManager.getLogger(AnalysisGraph.class);

  /**
   * The AnalysisFlowField only contains the attached leaves. The leaves contains their downstream
   * nodes. This way the entire graph can be rendered from just the leaves. For the Rca Analysis,
   * the metrics form the leaf nodes. They don't depend on anything else.
   */
  private List<Metric> metricList;

  public AnalysisGraph() {
    metricList = new ArrayList<>();
  }

  protected void addLeaf(Metric leaf) {
    metricList.add(leaf);
    leaf.setAddedToFlowField();
    Stats stats = Stats.getInstance();
    stats.incrementLeavesAddedToAnalysisFlowField();
    ConnectedComponent connectedComponent = new ConnectedComponent(stats.getGraphsCount());
    stats.addNewGraph(connectedComponent);
    leaf.setGraphId(connectedComponent.getGraphId());
    LOG.debug(
        "#rca: adding leafNode: {} with connected component id: {}",
        leaf.name(),
        leaf.getGraphId());
    LOG.debug("#rca: current connected component id: {}", connectedComponent.getGraphId());

    for (int i = 0; i < leaf.getGraphId(); ++i) {
      ConnectedComponent connectedComponent1 = stats.getGraphById(i);
      if (connectedComponent1 != null) {
        LOG.debug("#rca: graph so far: {}", connectedComponent1.getGraphId());
      }
    }
  }

  /**
   * This method should be called after calling the construct. The idea is the creator of the
   * FlowField should validate whether the Graphs are created in a way the Rca system expects and if
   * they can be run. If the validation passes, then it adds the leaf nodes to the graph. So, think
   * of it this way, a flow field is an aggregation of multiple graphs and each ConnectedComponent
   * is a connected component.
   */
  public void validateAndProcess() {
    for (Metric metricNode : metricList) {
      LOG.debug(
          "#rca: validateAndProcess: Metric: name {}, graph-id: {}",
          metricNode.name(),
          metricNode.getGraphId());
      ConnectedComponent connectedComponent =
          Stats.getInstance().getGraphById(metricNode.getGraphId());
      connectedComponent.addLeafNode(metricNode);
    }
  }

  /**
   * This is the entry point into graph creation. The RCA evaluation is a two step process. 1.
   * Create the classes by extending the Symptoms and Rca classes. 2. Extend the AnalysisGraph and
   * fill in the construct method. In construct method, you specify how different Rcas and Symptoms
   * are linked together. If an Rca or Symptom depends on other Rcas or symptoms, the second group
   * is the dependency of the Rca. 3. For each Rca and Symptom, override the operate method. An
   * operate method is passed a map of dependencies which are key-ed by the class of what you
   * specified in the construct method. For each dependency, you get a list of samples. And then you
   * use these samples to operate the Rca or Symptom. For evaluation you can also use the
   * NumericAggregator's static helper methods.
   */
  public abstract void construct();

  public void getConnectedComponents() {}
}
