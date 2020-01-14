package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse.SubscriptionStatus;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SubscriptionReceiverTask implements Runnable {

  private static final Logger LOG = LogManager.getLogger(SubscriptionReceiverTask.class);
  private final SubscriptionManager subscriptionManager;
  private final NetworkQueue<CompositeSubscribeRequest> rxQ;

  public SubscriptionReceiverTask(final SubscriptionManager subscriptionManager,
      final NetworkQueue<CompositeSubscribeRequest> rxQ) {
    this.subscriptionManager = subscriptionManager;
    this.rxQ = rxQ;
  }
  /**
   * When an object implementing interface <code>Runnable</code> is used to create a thread,
   * starting the thread causes the object's
   * <code>run</code> method to be called in that separately executing
   * thread.
   * <p>
   * The general contract of the method <code>run</code> is that it may take any action whatsoever.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    for (final CompositeSubscribeRequest compositeSubscribeRequest : rxQ.drain()) {
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
}
