/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages;

import java.util.Map;

public class IntentMsg {
  /**
   * The node sending the intent. This is the node whose one or more dependency is not locally
   * available.
   */
  String requesterGraphNode;

  /** The name of the destination node whose data is desired. */
  String destinationGraphNode;

  /**
   * The requesting node's rca.conf tags. This tags will be used by the requested Node's network
   * thread to send data.
   */
  Map<String, String> rcaConfTags;

  public String getRequesterGraphNode() {
    return requesterGraphNode;
  }

  public String getDestinationGraphNode() {
    return destinationGraphNode;
  }

  public Map<String, String> getRcaConfTags() {
    return rcaConfTags;
  }

  public IntentMsg(String requesterGraphNode, String destinationGraphNode, Map<String, String> rcaConfTags) {
    this.requesterGraphNode = requesterGraphNode;
    this.destinationGraphNode = destinationGraphNode;
    this.rcaConfTags = rcaConfTags;
  }

  @Override
  public String toString() {
    return String.format("Intent::from: '%s', to: '%s'", requesterGraphNode, destinationGraphNode);
  }
}
