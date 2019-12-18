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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericContext;

public class SymptomContext extends GenericContext {
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

  private State state;

  public SymptomContext(State state) {
    this.state = state;
  }

  public State getState() {
    return this.state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public boolean isUnhealthy() {
    return this.state == State.UNHEALTHY || this.state == State.CONTENDED;
  }

  public static SymptomContext generic() {
    return new SymptomContext(State.UNKNOWN);
  }

  @Override
  public String toString() {
    return this.state.toString();
  }
}
