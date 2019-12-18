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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericContext;

public class ResourceContext extends GenericContext {
  public enum State {
    HEALTHY("healthy"),
    UNHEALTHY("unhealthy"),
    CONTENDED("contended"),
    UNKNOWN("unknown");
    private String stateName;

    private State(String stateName) {
      this.stateName = stateName;
    }

    @Override
    public String toString() {
      return this.stateName;
    }
  }

  public enum Resource {
    CPU("CPU"),
    MEM("Memory"),
    JVM("JVM"),
    DISK("disk"),
    CACHE("CACHE"),
    NIC("NIC"),
    HEAP("heap"),
    SHARD("shard"),
    UNKNOWN("unknown");
    private String resourceName;

    private Resource(String resourceName) {
      this.resourceName = resourceName;
    }

    @Override
    public String toString() {
      return this.resourceName;
    }
  }

  private State state;
  private Resource resource;

  public ResourceContext(Resource resource, State state) {
    this.resource = resource;
    this.state = state;
  }

  public State getState() {
    return this.state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public Resource getResource() {
    return this.resource;
  }

  public void setResource(Resource resource) {
    this.resource = resource;
  }

  public boolean isUnhealthy() {
    return this.state == State.UNHEALTHY || this.state == State.CONTENDED;
  }

  public boolean isUnknown() {
    return this.state == State.UNKNOWN;
  }

  public static ResourceContext generic() {
    return new ResourceContext(Resource.UNKNOWN, State.UNKNOWN);
  }

  public FlowUnitMessage.ResourceContextMessage buildContextMessage() {
    final FlowUnitMessage.ResourceContextMessage.Builder contextMessageBuilder =
        FlowUnitMessage.ResourceContextMessage.newBuilder();
    contextMessageBuilder.setState(state.ordinal());
    contextMessageBuilder.setResource(resource.ordinal());
    return contextMessageBuilder.build();
  }

  public static ResourceContext buildResourceContextFromMessage(
      FlowUnitMessage.ResourceContextMessage message) {
    return new ResourceContext(
        Resource.values()[message.getResource()], State.values()[message.getState()]);
  }

  @Override
  public String toString() {
    return this.resource + " " + this.state;
  }
}
