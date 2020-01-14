package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.UnicastIntentMsg;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class responsible for enqueueing subscription requests to be sent.
 */
public class SubscriptionSender {
  private final NetworkRequestQueue<IntentMsg> txBroadcastQ;
  private final NetworkRequestQueue<UnicastIntentMsg> txUnicastQ;
  private final SubscriptionSendTask subscriptionSendTask;
  private final ScheduledExecutorService threadPool;

  public SubscriptionSender(
      NetworkRequestQueue<IntentMsg> txBroadcastQ,
      NetworkRequestQueue<UnicastIntentMsg> txUnicastQ,
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
    threadPool.scheduleAtFixedRate(subscriptionSendTask, 5000, 250, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    // drain out the queue to stop processing.
    txBroadcastQ.drain();
    txUnicastQ.drain();
  }
}
