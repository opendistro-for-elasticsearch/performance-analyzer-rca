package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import java.util.Comparator;

class SortByIngressOrder implements Comparator<Node> {

  @Override
  public int compare(Node o1, Node o2) {
    return o1.getUpStreamNodesCount() - o2.getUpStreamNodesCount();
  }
}
