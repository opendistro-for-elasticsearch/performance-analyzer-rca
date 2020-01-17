package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util.ClusterUtils;
import java.util.Map;

public class BroadcastSubscriptionTxTask extends SubscriptionTxTask {

  public BroadcastSubscriptionTxTask(
      NetClient netClient,
      IntentMsg intentMsg,
      SubscriptionManager subscriptionManager) {
    super(netClient, intentMsg, subscriptionManager);
  }

  /**
   * Broadcasts a subscription request to all the peers in the cluster.
   * @see Thread#run()
   */
  @Override
  public void run() {
    final String requesterVertex = intentMsg.getRequesterNode();
    final String destinationVertex = intentMsg.getDestinationNode();
    final Map<String, String> tags = intentMsg.getRcaConfTags();

    for (final String remoteHost : ClusterUtils.getAllPeerHostAddresses()) {
      sendSubscribeRequest(remoteHost, requesterVertex, destinationVertex, tags);
    }
  }
}
