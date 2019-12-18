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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.StringList;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import java.util.List;

public class ResourceFlowUnit extends GenericFlowUnit {
  private ResourceContext resourceContext = null;

  public ResourceFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public ResourceFlowUnit(long timeStamp, ResourceContext context) {
    super(timeStamp);
    this.resourceContext = context;
  }

  public ResourceFlowUnit(long timeStamp, List<List<String>> data, ResourceContext context) {
    super(timeStamp, data);
    this.resourceContext = context;
  }

  public ResourceContext getResourceContext() {
    return this.resourceContext;
  }

  // Call generic() only if you want to generate a empty flowunit
  public static ResourceFlowUnit generic() {
    return new ResourceFlowUnit(System.currentTimeMillis());
  }

  public FlowUnitMessage buildFlowUnitMessage(final String graphNode, final String esNode) {
    final FlowUnitMessage.Builder messageBuilder = FlowUnitMessage.newBuilder();
    messageBuilder.setGraphNode(graphNode);
    messageBuilder.setEsNode(esNode);

    if (!this.isEmpty()) {
      for (List<String> value : this.getData()) {
        messageBuilder.addValues(StringList.newBuilder().addAllValues(value).build());
      }
    }

    messageBuilder.setTimestamp(System.currentTimeMillis());
    if (resourceContext != null) {
      messageBuilder.setResourceContext(resourceContext.buildContextMessage());
    }
    return messageBuilder.build();
  }

  public static ResourceFlowUnit buildFlowUnitFromWrapper(final FlowUnitWrapper value) {
    if (value.hasData()) {
      return new ResourceFlowUnit(
          value.getTimeStamp(), value.getData(), value.getResourceContext());
    } else {
      return new ResourceFlowUnit(value.getTimeStamp(), value.getResourceContext());
    }
  }

  @Override
  public String toString() {
    return String.format("%d: %s :: %s", this.getTimeStamp(), this.getData(), resourceContext);
  }
}
