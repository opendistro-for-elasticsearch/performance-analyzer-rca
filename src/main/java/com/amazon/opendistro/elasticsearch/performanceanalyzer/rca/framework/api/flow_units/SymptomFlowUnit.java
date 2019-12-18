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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.SymptomContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import java.util.List;

public class SymptomFlowUnit extends GenericFlowUnit {
  private SymptomContext context = null;

  public SymptomFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public SymptomFlowUnit(long timeStamp, SymptomContext context) {
    super(timeStamp);
    this.context = context;
  }

  public SymptomFlowUnit(long timeStamp, List<List<String>> data, SymptomContext context) {
    super(timeStamp, data);
    this.context = context;
  }

  public SymptomContext getContext() {
    return this.context;
  }

  public static SymptomFlowUnit generic() {
    return new SymptomFlowUnit(System.currentTimeMillis());
  }

  public FlowUnitMessage buildFlowUnitMessage(final String graphNode, final String esNode) {
    final FlowUnitMessage.Builder messageBuilder = FlowUnitMessage.newBuilder();
    messageBuilder.setGraphNode(graphNode);
    messageBuilder.setEsNode(esNode);

    messageBuilder.setTimestamp(System.currentTimeMillis());

    return messageBuilder.build();
  }

  @Override
  public String toString() {
    return String.format("%d", this.getTimeStamp());
  }
}
