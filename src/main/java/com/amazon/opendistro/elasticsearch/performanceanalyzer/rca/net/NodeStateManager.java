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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeStateManager {
  private static final long MS_IN_S = 1000;
  private static final long MS_IN_FIVE_SECONDS = 5 * MS_IN_S;
  private static final String SEPARATOR = ".";

  private Map<String, Long> lastReceivedTimestampMap = new HashMap<>();

  public void updateReceiveTime(String host, String graphNode) {
    final long currentTimeStamp = System.currentTimeMillis();
    final String compositeKey = graphNode + SEPARATOR + host;
    lastReceivedTimestampMap.put(compositeKey, currentTimeStamp);
  }

  public long getLastReceivedTimestamp(String graphNode, String host) {
    final String compositeKey = graphNode + SEPARATOR + host;
    if (lastReceivedTimestampMap.containsKey(compositeKey)) {
      return lastReceivedTimestampMap.get(compositeKey);
    }

    // Return a value that is in the future so that it doesn't cause
    // side effects.
    return System.currentTimeMillis() + MS_IN_FIVE_SECONDS;
  }

  public boolean isRemoteHostInCluster(final String remoteHost) {
    final List<NodeDetails> nodes = ClusterDetailsEventProcessor.getNodesDetails();

    if (nodes.size() > 0) {
      for (NodeDetails node : nodes) {
        if (node.getHostAddress()
            .equals(remoteHost)) {
          return true;
        }
      }
    }

    return false;
  }
}
