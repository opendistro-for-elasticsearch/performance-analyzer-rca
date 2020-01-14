package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Sender {

  private final NetworkQueue<DataMsg> txQ;
  private final SendTask sendTask;
  private final ScheduledExecutorService threadPool;

  public Sender(final NetworkQueue<DataMsg> txQ,
      final SendTask sendTask,
      final ScheduledExecutorService threadPool) {
    this.txQ = txQ;
    this.sendTask = sendTask;
    this.threadPool = threadPool;
  }

  public void start() {
    threadPool.scheduleAtFixedRate(sendTask, 0, 250, TimeUnit.MILLISECONDS);
  }

  public boolean enqueue(final DataMsg dataMsg) {
    return txQ.offer(dataMsg);
  }
}
