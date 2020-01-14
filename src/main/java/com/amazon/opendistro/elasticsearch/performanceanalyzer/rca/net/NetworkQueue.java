package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class that represents the queue of messages that need to be sent out.
 */
public class NetworkQueue<T> {

  private static final Logger LOG = LogManager.getLogger(NetworkQueue.class);
  private static final int MAX_Q_SIZE = 200;

  private BlockingQueue<T> dataMsgBlockingQueue = new ArrayBlockingQueue<>(MAX_Q_SIZE);

  /**
   * Adds a message to the queue if not full.
   *
   * @param message The message that needs to be enqueued..
   * @return true if successfully enqueued, false otherwise.
   */
  public synchronized boolean offer(final T message) {
    // happens-before (java.util.concurrent collection)
    return dataMsgBlockingQueue.offer(message);
  }

  /**
   * Drains the queue to an immutable list. Can get called from a different thread from the one that
   * wrote to the queue.
   *
   * @return Immutable list containing the contents of the queue.
   */
  public ImmutableList<T> drain() {
    final List<T> dataMsgList = new ArrayList<>();
    // happens-before (same thread)
    dataMsgBlockingQueue.drainTo(dataMsgList);

    // happens-before (final-field inside ImmutableList creation)
    return ImmutableList.copyOf(dataMsgList);
  }
}
