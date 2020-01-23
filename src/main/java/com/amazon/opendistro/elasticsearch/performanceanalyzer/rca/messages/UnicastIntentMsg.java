package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages;

import java.util.Map;

public class UnicastIntentMsg extends IntentMsg {

  private final String unicastDestinationHostAddress;

  public UnicastIntentMsg(String requesterNode, String destinationNode,
      Map<String, String> rcaConfTags, String unicastDestinationHostAddress) {
    super(requesterNode, destinationNode, rcaConfTags);
    this.unicastDestinationHostAddress = unicastDestinationHostAddress;
  }

  public String getUnicastDestinationHostAddress() {
    return unicastDestinationHostAddress;
  }
}
