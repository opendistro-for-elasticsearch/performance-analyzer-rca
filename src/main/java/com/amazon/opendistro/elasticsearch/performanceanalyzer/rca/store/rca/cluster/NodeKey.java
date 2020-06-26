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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class NodeKey {
  private final String nodeId;
  private final String hostAddress;

  public NodeKey(String nodeId, String hostAddress) {
    this.nodeId = nodeId;
    this.hostAddress = hostAddress;
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof NodeKey) {
      NodeKey key = (NodeKey)obj;
      return nodeId.equals(key.getNodeId()) && hostAddress.equals(key.getHostAddress());
    }
    return false;
  }

  // the reason why we compare both node id and  hostAddress here is because in
  // newer ES version(6.8 and above), see https://github.com/elastic/elasticsearch/pull/19140.
  // if es restart, both node id and ip address will remain the same so we can continue add
  // flowunit into the same row in table before es restart.
  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(nodeId)
        .append(hostAddress)
        .toHashCode();
  }

  @Override
  public String toString() {
    return nodeId + " " + hostAddress;
  }
}
