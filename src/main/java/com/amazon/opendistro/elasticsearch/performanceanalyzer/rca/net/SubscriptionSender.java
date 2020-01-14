package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.UnicastIntentMsg;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SubscriptionSender {
  private final NetworkQueue<IntentMsg> txBroadcastQ;
  private final NetworkQueue<UnicastIntentMsg> txUnicastQ;
  private final SubscriptionSendTask subscriptionSendTask;
  private final ScheduledExecutorService threadPool;

  public SubscriptionSender(
      NetworkQueue<IntentMsg> txBroadcastQ,
      NetworkQueue<UnicastIntentMsg> txUnicastQ,
      SubscriptionSendTask subscriptionSendTask,
      ScheduledExecutorService threadPool) {
    this.txBroadcastQ = txBroadcastQ;
    this.txUnicastQ = txUnicastQ;
    this.subscriptionSendTask = subscriptionSendTask;
    this.threadPool = threadPool;
  }

  public boolean enqueueForBroadcast(final IntentMsg intentMsg) {
    return txBroadcastQ.offer(intentMsg);
  }

  public boolean enqueueForUnicast(final UnicastIntentMsg intentMsg) {
    return txUnicastQ.offer(intentMsg);
  }

  public void start() {
    threadPool.scheduleAtFixedRate(subscriptionSendTask, 0, 250, TimeUnit.MILLISECONDS);
  }
}
