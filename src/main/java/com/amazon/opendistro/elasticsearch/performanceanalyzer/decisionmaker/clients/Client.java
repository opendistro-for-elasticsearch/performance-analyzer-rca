/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.clients;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;

/**
 * Client interface that Publisher connected with to publish remediation requests
 */
public interface Client {

  /**
   * create a new batch request
   */
  void newBatchRequest();

  /**
   * Create a single request and append itself to the batch request
   * @param nodeKey target instance of the action
   * @param resourceEnum resource type of the action
   * @param payload json payload that is sent to target
   */
  void addRequest(NodeKey nodeKey, ResourceEnum resourceEnum, String payload);

  /**
   * send out the batch request to server
   */
  void sendBatchRequest();
}
