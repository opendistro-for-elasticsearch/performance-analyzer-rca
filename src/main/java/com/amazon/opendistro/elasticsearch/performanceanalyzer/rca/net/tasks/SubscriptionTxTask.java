package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscribeResponseHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util.ClusterUtils;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class SubscriptionTxTask implements Runnable {

  private static final Logger LOG = LogManager.getLogger(SubscriptionTxTask.class);
  protected final NetClient netClient;
  protected final IntentMsg intentMsg;
  protected final SubscriptionManager subscriptionManager;

  public SubscriptionTxTask(
      final NetClient netClient,
      final IntentMsg intentMsg,
      final SubscriptionManager subscriptionManager) {
    this.netClient = netClient;
    this.intentMsg = intentMsg;
    this.subscriptionManager = subscriptionManager;
  }

  protected void sendSubscribeRequest(final String remoteHost, final String requesterVertex,
      final String destinationVertex, final Map<String, String> tags) {
    final SubscribeMessage subscribeMessage = SubscribeMessage.newBuilder()
                                                              .setDestinationNode(destinationVertex)
                                                              .setRequesterNode(requesterVertex)
                                                              .putTags("locus", tags.get("locus"))
                                                              .putTags("requester",
                                                                  ClusterUtils
                                                                      .getCurrentNodeHostAddress())
                                                              .build();
    netClient.subscribe(remoteHost, subscribeMessage,
        new SubscribeResponseHandler(subscriptionManager, remoteHost, destinationVertex));
  }
}
