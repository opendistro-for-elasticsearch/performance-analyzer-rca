package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import java.util.Map;

public class UnicastIntentMsg extends IntentMsg {

  private final InstanceDetails unicastDestinationInstance;

  public UnicastIntentMsg(String requesterNode, String destinationNode,
      Map<String, String> rcaConfTags, InstanceDetails unicastDestinationInstance) {
    super(requesterNode, destinationNode, rcaConfTags);
    this.unicastDestinationInstance = unicastDestinationInstance;
  }

  public InstanceDetails getUnicastDestinationInstance() {
    return unicastDestinationInstance;
  }
}
