package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.UnicastIntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class that handles sending of subscription requests from the queue.
 */
public class SubscriptionSendTask implements Runnable {

  private static final Logger LOG = LogManager.getLogger(SubscriptionSendTask.class);
  private final SubscriptionManager subscriptionManager;
  private final NetworkRequestQueue<IntentMsg> txBroadcastQ;
  private final NetworkRequestQueue<UnicastIntentMsg> txUnicastQ;
  private final NetClient netClient;

  public SubscriptionSendTask(final SubscriptionManager subscriptionManager,
      final NetworkRequestQueue<IntentMsg> txBroadcastQ, final NetworkRequestQueue<UnicastIntentMsg> txUnicastQ,
      final NetClient netClient) {
    this.subscriptionManager = subscriptionManager;
    this.txBroadcastQ = txBroadcastQ;
    this.txUnicastQ = txUnicastQ;
    this.netClient = netClient;
  }

  /**
   * When an object implementing interface <code>Runnable</code> is used to create a thread,
   * starting the thread causes the object's
   * <code>run</code> method to be called in that separately executing
   * thread.
   *
   * <p>The general contract of the method <code>run</code> is that it may take any action whatsoever.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    // Handle broadcasting subscription.
    for (final IntentMsg intentMsg : txBroadcastQ.drain()) {
      LOG.info("kk: Draining subscribe-broadcast TxQ");
      final String requesterGraphNode = intentMsg.getRequesterNode();
      final String destinationGraphNode = intentMsg.getDestinationNode();
      final Map<String, String> tags = intentMsg.getRcaConfTags();

      for (final String remoteHost : getAllRemoteHosts()) {
        sendSubscribeRequest(remoteHost, requesterGraphNode, destinationGraphNode, tags);
      }
    }

    // Handle unicasting
    for (final UnicastIntentMsg intentMsg : txUnicastQ.drain()) {
      LOG.info("kk: Draining subscribe-unicast TxQ");
      final String requesterGraphNode = intentMsg.getRequesterNode();
      final String destinationGraphNode = intentMsg.getDestinationNode();
      final Map<String, String> tags = intentMsg.getRcaConfTags();
      final String destinationHostAddress = intentMsg.getUnicastDestinationHostAddress();

      sendSubscribeRequest(destinationHostAddress, requesterGraphNode, destinationGraphNode, tags);
    }
  }

  private void sendSubscribeRequest(final String remoteHost, final String requesterGraphNode,
      final String destinationGraphNode, final Map<String, String> tags) {
    final SubscribeMessage subscribeMessage = SubscribeMessage.newBuilder()
                                                              .setDestinationNode(
                                                                  destinationGraphNode)
                                                              .setRequesterNode(requesterGraphNode)
                                                              .putTags("locus", tags.get("locus"))
                                                              .putTags("requester",
                                                                  getCurrentHostAddress())
                                                              .build();

    netClient.subscribe(remoteHost, subscribeMessage,
        new SubscribeResponseHandler(subscriptionManager, remoteHost, destinationGraphNode));
  }

  /**
   * Read the NodeDetails of all the remote nodes skip the first node in the list because it is
   * local node that this is currently running on.
   */
  private List<String> getAllRemoteHosts() {
    return ClusterDetailsEventProcessor.getNodesDetails().stream()
                                       .skip(1)
                                       .map(NodeDetails::getHostAddress)
                                       .collect(Collectors.toList());
  }

  private String getCurrentHostAddress() {
    return Objects.requireNonNull(ClusterDetailsEventProcessor.getCurrentNodeDetails())
                  .getHostAddress();
  }
}
