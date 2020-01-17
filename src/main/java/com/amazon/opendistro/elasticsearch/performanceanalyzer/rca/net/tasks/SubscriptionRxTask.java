package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse.SubscriptionStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.CompositeSubscribeRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SubscriptionRxTask implements Runnable {

  private static final Logger LOG = LogManager.getLogger(SubscriptionRxTask.class);
  private final SubscriptionManager subscriptionManager;
  private final CompositeSubscribeRequest compositeSubscribeRequest;

  public SubscriptionRxTask(
      final SubscriptionManager subscriptionManager,
      final CompositeSubscribeRequest compositeSubscribeRequest) {
    this.subscriptionManager = subscriptionManager;
    this.compositeSubscribeRequest = compositeSubscribeRequest;
  }

  /**
   * Process the subscription request.
   * @see Thread#run()
   */
  @Override
  public void run() {
    final SubscribeMessage request = compositeSubscribeRequest.getSubscribeMessage();
    final Map<String, String> tags = request.getTagsMap();
    final String requesterHostAddress = tags.getOrDefault("requester", "");
    final String locus = tags.getOrDefault("locus", "");
    final SubscriptionStatus subscriptionStatus =
        subscriptionManager
            .addSubscriber(request.getDestinationNode(), requesterHostAddress, locus);

    final StreamObserver<SubscribeResponse> responseStream = compositeSubscribeRequest
        .getSubscribeResponseStream();
    responseStream.onNext(SubscribeResponse.newBuilder()
                                           .setSubscriptionStatus(subscriptionStatus)
                                           .build());
    responseStream.onCompleted();
  }
}
