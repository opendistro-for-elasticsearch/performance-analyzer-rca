/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages the subscription state for the nodes in the graph.
 */
public class NodeStateManager {

  private static final String SEPARATOR = ".";

  private ConcurrentMap<String, AtomicLong> lastReceivedTimestampMap = new ConcurrentHashMap<>();

  /**
   * Updates the timestamp for the composite key: (host, vertex) marking when the last successful
   * flow unit reception happened.
   *
   * @param host      The host that sent the flow unit.
   * @param graphNode The vertex for which the flow unit was sent for.
   * @param timestamp The timestamp at which we received.
   */
  public void updateReceiveTime(final String host, final String graphNode, final long timestamp) {
    final String compositeKey = graphNode + SEPARATOR + host;
    AtomicLong existingLong = lastReceivedTimestampMap.get(compositeKey);
    if (existingLong == null) {
      // happens-before: updating a java.util.concurrent collection. Update is made visible to
      // all threads that read this collection.
      AtomicLong prevVal = lastReceivedTimestampMap
          .putIfAbsent(compositeKey, new AtomicLong(timestamp));
      if (prevVal != null) {
        // happens-before: updating AtomicLong. Update is made visible to all threads that
        // read this atomic long.
        lastReceivedTimestampMap.get(compositeKey).set(timestamp);
      }
    } else {
      // happens-before: updating AtomicLong. Update is made visible to all threads that
      // read this atomic long.
      lastReceivedTimestampMap.get(compositeKey).set(timestamp);
    }
  }

  /**
   * Retrieves the latest timestamp at which we received a flow unit from this host for this
   * vertex.
   *
   * @param graphNode The vertex for which we need the last received time stamp for.
   * @param host      The host for which we need the last received timestamp for, for the vertex.
   * @return The timestamp at which we received a flow unit from the host for the vertex if present,
   *         a timestamp in the distant past(0) otherwise.
   */
  public long getLastReceivedTimestamp(String graphNode, String host) {
    final String compositeKey = graphNode + SEPARATOR + host;
    if (lastReceivedTimestampMap.containsKey(compositeKey)) {
      return lastReceivedTimestampMap.get(compositeKey).get();
    }

    // Return a value that is in the distant past.
    return 0;
  }
}
