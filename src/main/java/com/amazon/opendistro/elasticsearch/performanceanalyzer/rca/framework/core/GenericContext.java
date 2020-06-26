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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;

public abstract class GenericContext {
    private final Resources.State state;

    public GenericContext(Resources.State state) {
        this.state = state;
    }

    public Resources.State getState() {
        return this.state;
    }

    public boolean isUnhealthy() {
        return this.state == Resources.State.UNHEALTHY || this.state == Resources.State.CONTENDED;
    }

    public boolean isHealthy() {
        return this.state == Resources.State.HEALTHY;
    }

    public boolean isUnknown() {
        return this.state == Resources.State.UNKNOWN;
    }

    @Override
    public String toString() {
        return this.state.toString();
    }
}
