package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.UnicastIntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import java.util.Map;

public class UnicastSubscriptionTxTask extends SubscriptionTxTask {

  private final String destinationHostAddress;

  public UnicastSubscriptionTxTask(
      NetClient netClient,
      UnicastIntentMsg intentMsg,
      SubscriptionManager subscriptionManager,
      NodeStateManager nodeStateManager) {
    super(netClient, intentMsg, subscriptionManager, nodeStateManager);
    this.destinationHostAddress = intentMsg.getUnicastDestinationHostAddress();
  }

  /**
   * Sends a subscription request to a known destination address.
   * @see Thread#run()
   */
  @Override
  public void run() {
    final String requesterVertex = intentMsg.getRequesterNode();
    final String destinationVertex = intentMsg.getDestinationNode();
    final Map<String, String> tags = intentMsg.getRcaConfTags();

    sendSubscribeRequest(destinationHostAddress, requesterVertex, destinationVertex, tags);
  }
}
