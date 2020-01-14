package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Class that represents a custom bounded and concurrent queue.
 */
public class NetworkRequestQueue<T> {

  private static final int DEFAULT_MAX_Q_SIZE = 200;

  private final BlockingQueue<T> messageQueue;

  public NetworkRequestQueue() {
    this(DEFAULT_MAX_Q_SIZE);
  }

  public NetworkRequestQueue(final int queueSize) {
    this.messageQueue = new ArrayBlockingQueue<>(queueSize);
  }

  /**
   * Adds a message to the queue if not full.
   *
   * @param message The message that needs to be enqueued..
   * @return true if successfully enqueued, false otherwise.
   */
  public boolean offer(final T message) {
    // happens-before (java.util.concurrent collection)
    return messageQueue.offer(message);
  }

  /**
   * Drains the queue to an immutable list. Can get called from a different thread from the one that
   * wrote to the queue.
   *
   * @return Immutable list containing the contents of the queue.
   */
  public ImmutableList<T> drain() {
    final List<T> messageList = new ArrayList<>();
    // happens-before (same lock)
    messageQueue.drainTo(messageList);

    // happens-before (final-field inside ImmutableList creation)
    return ImmutableList.copyOf(messageList);
  }
}
