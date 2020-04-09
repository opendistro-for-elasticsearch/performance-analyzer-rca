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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceContextMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericContext;
import java.util.ArrayList;
import java.util.List;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * context that goes along with ResourceFlowUnit.
 */
public class ResourceContext extends GenericContext {
  public static final String STATE_COL_NAME = "State";

  public ResourceContext(Resources.State state) {
    super(state);
  }

  public static ResourceContext generic() {
    return new ResourceContext(Resources.State.UNKNOWN);
  }

  public ResourceContextMessage buildContextMessage() {
    final ResourceContextMessage.Builder contextMessageBuilder = ResourceContextMessage
        .newBuilder();
    contextMessageBuilder.setState(getState().ordinal());
    return contextMessageBuilder.build();
  }

  public static ResourceContext buildResourceContextFromMessage(ResourceContextMessage message) {
    return new ResourceContext(Resources.State.values()[message.getState()]);
  }

  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schemaList = new ArrayList<>();
    schemaList.add(DSL.field(DSL.name(STATE_COL_NAME), String.class));
    return schemaList;
  }

  public List<Object> getSqlValue() {
    List<Object> valueList = new ArrayList<>();
    valueList.add(getState().toString());
    return valueList;
  }
}
