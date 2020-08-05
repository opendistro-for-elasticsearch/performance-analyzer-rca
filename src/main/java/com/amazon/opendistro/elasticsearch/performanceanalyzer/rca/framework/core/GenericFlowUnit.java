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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;

// TODO: Doc comments and a description of each member.
public abstract class GenericFlowUnit {

  private long timeStamp;
  protected boolean empty;

  // Creates an empty flow unit.
  public GenericFlowUnit(long timeStamp) {
    this.empty = true;
    this.timeStamp = timeStamp;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public boolean isEmpty() {
    return this.empty;
  }

  public abstract FlowUnitMessage buildFlowUnitMessage(final String graphNode, final InstanceDetails.Id esNode);
}
