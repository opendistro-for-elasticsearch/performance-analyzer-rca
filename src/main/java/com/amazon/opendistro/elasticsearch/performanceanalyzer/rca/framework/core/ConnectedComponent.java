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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConnectedComponent {
  private List<Node<?>> leafNodes;

  /* The elements in the inner list can be executed in parallel. Two inner lists have to be executed in order. */
  private List<List<Node<?>>> dependencyOrderedNodes;
  private int graphId;

  private Set<Node<?>> getAllNodes() {
    Set<Node<?>> traversed = new HashSet<>();
    Deque<Node<?>> inline = new ArrayDeque<>(leafNodes);
    while (!inline.isEmpty()) {
      Node<?> currNode = inline.poll();
      if (traversed.contains(currNode)) {
        continue;
      }
      traversed.add(currNode);
      List<Node<?>> currNodesDownstream = currNode.getDownStreams();
      if (currNodesDownstream.size() > 0) {
        inline.addAll(currNodesDownstream);
      }
    }
    return traversed;
  }

  public ConnectedComponent(int graphId) {
    this.leafNodes = new ArrayList<>();
    this.graphId = graphId;
  }

  public int getGraphId() {
    return graphId;
  }

  public void addLeafNode(Node node) {
    leafNodes.add(node);
  }

  public List<List<Node<?>>> getAllNodesByDependencyOrder() {
    if (dependencyOrderedNodes != null) {
      return dependencyOrderedNodes;
    }
    List<Node<?>> allNodes = new ArrayList<>(getAllNodes());
    allNodes.sort(new SortByIngressOrder());

    dependencyOrderedNodes = new ArrayList<>(allNodes.size());

    int[] ingressCountArray = new int[allNodes.size()];

    // A list of nodes which have no incoming edges.
    Deque<Node<?>> zeroIngressNodes = new ArrayDeque<>(allNodes.size() / 2);

    // A map to map a node to its position in the ingressCountArray, for fast retrieval.
    Map<Node<?>, Integer> nodePositionMap = new HashMap<>(allNodes.size());

    int index = 0;

    // Loop to initiate the ingressCountArray. For each node, as it is positioned in the allNodes
    // list,
    // the value is the number of incoming edges to the node.
    for (Node<?> node : allNodes) {
      int upStreamNodesCount = node.getUpStreamNodesCount();
      if (upStreamNodesCount == 0) {
        zeroIngressNodes.add(node);
      }
      ingressCountArray[index] = upStreamNodesCount;
      nodePositionMap.put(node, index);
      ++index;
    }

    while (!zeroIngressNodes.isEmpty()) {
      List<Node<?>> innerList = new ArrayList<>(zeroIngressNodes);
      dependencyOrderedNodes.add(innerList);
      zeroIngressNodes.clear();

      for (Node<?> node : innerList) {
        // For all the nodes downstream of this node, decrement the ingress count.
        for (Node<?> downstreamNode : node.getDownStreams()) {
          int pos = nodePositionMap.get(downstreamNode);
          --ingressCountArray[pos];
          if (ingressCountArray[pos] == 0) {
            // If decrementing the ingress, brings down the ingress to zero, then this is the new
            // contender
            // of the zeroIngressNodes list.
            zeroIngressNodes.addLast(downstreamNode);
          }
        }
      }
    }
    return dependencyOrderedNodes;
  }
}
