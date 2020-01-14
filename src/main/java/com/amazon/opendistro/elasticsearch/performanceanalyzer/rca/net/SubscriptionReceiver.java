package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SubscriptionReceiver {

  private static final Logger LOG = LogManager.getLogger(SubscriptionReceiver.class);
  private final NetworkQueue<CompositeSubscribeRequest> rxQ;
  private final ScheduledExecutorService threadPool;
  private final SubscriptionReceiverTask subscriptionReceiverTask;

  public SubscriptionReceiver(
      NetworkQueue<CompositeSubscribeRequest> rxQ,
      ScheduledExecutorService threadPool,
      SubscriptionReceiverTask subscriptionReceiverTask) {
    this.rxQ = rxQ;
    this.threadPool = threadPool;
    this.subscriptionReceiverTask = subscriptionReceiverTask;
  }

  public boolean enqueue(final CompositeSubscribeRequest compositeSubscribeRequest) {
    return rxQ.offer(compositeSubscribeRequest);
  }

  public void start() {
    threadPool.scheduleAtFixedRate(subscriptionReceiverTask, 0, 250, TimeUnit.MILLISECONDS);
  }
}
