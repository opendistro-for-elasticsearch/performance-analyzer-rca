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
import java.util.List;

// TODO: Doc comments and a description of each member.
public abstract class GenericFlowUnit {
  private long timeStamp;
  private boolean empty = true;
  private List<List<String>> data = null;

  // Creates an empty flow unit.
  public GenericFlowUnit(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public GenericFlowUnit(long timeStamp, List<List<String>> data) {
    this(timeStamp);
    this.data = data;
    this.empty = false;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public boolean isEmpty() {
    return empty;
  }

  public List<List<String>> getData() {
    return data;
  }

  public abstract FlowUnitMessage buildFlowUnitMessage(final String graphNode, final String esNode);
}
