package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages;

import java.util.Map;

public class IntentMsg {
  /**
   * The node sending the intent. This is the node whose one or more dependency is not locally
   * available.
   */
  String requesterNode;

  /** The name of the destination node whose data is desired. */
  String destinationNode;

  /**
   * The requesting node's rca.conf tags. This tags will be used by the requested Node's network
   * thread to send data.
   */
  Map<String, String> rcaConfTags;

  public IntentMsg(String requesterNode, String destinationNode, Map<String, String> rcaConfTags) {
    this.requesterNode = requesterNode;
    this.destinationNode = destinationNode;
    this.rcaConfTags = rcaConfTags;
  }

  public String getRequesterNode() {
    return requesterNode;
  }

  public String getDestinationNode() {
    return destinationNode;
  }

  public Map<String, String> getRcaConfTags() {
    return rcaConfTags;
  }

  @Override
  public String toString() {
    return String.format("Intent::from: '%s', to: '%s'", requesterNode, destinationNode);
  }
}
