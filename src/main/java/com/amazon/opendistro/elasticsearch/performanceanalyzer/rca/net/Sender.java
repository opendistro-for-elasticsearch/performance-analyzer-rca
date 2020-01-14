package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Networking component that handles sending of flow units.
 */
public class Sender {

  private final NetworkRequestQueue<DataMsg> txQ;
  private final SendTask sendTask;
  private final ScheduledExecutorService threadPool;

  public Sender(final NetworkRequestQueue<DataMsg> txQ,
      final SendTask sendTask,
      final ScheduledExecutorService threadPool) {
    this.txQ = txQ;
    this.sendTask = sendTask;
    this.threadPool = threadPool;
  }

  public void start() {
    threadPool.scheduleAtFixedRate(sendTask, 0, 250, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    // Drain out the messages to stop processing.
    txQ.drain();
  }

  public boolean enqueue(final DataMsg dataMsg) {
    return txQ.offer(dataMsg);
  }
}
