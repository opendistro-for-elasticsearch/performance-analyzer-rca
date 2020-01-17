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
import com.google.protobuf.GeneratedMessageV3;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.jooq.Field;

public abstract class GenericSummary {

  public GenericSummary() {
    nestedSummaryList = new ArrayList<>();
  }

  protected final List<GenericSummary> nestedSummaryList;

  public List<GenericSummary> getNestedSummaryList() {
    return nestedSummaryList;
  }

  public void addNestedSummaryList(Collection<GenericSummary> nestedSummaryList) {
    this.nestedSummaryList.addAll(nestedSummaryList);
  }

  public void addNestedSummaryList(GenericSummary nestedSummary) {
    this.nestedSummaryList.add(nestedSummary);
  }

  public abstract <T extends GeneratedMessageV3> T buildSummaryMessage();

  public abstract void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder);

  public abstract List<Field<?>> getSqlSchema();

  public abstract List<Object> getSqlValue();
}
