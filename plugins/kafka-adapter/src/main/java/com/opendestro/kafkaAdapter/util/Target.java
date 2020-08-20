/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.opendestro.kafkaAdapter.util;

public class Target {
    private String endpoint;
    private String parameter;
    private String url;

    public Target(String endpoint, String parameter) {
        this.endpoint = endpoint;
        this.parameter = parameter;
        this.url = "http://" + this.endpoint + "?name=" + this.parameter;
    }

    public Target(String endpoint) {
        this.endpoint = endpoint;
        this.url = "http://" + this.endpoint;
    }

    public String getUrl() {
        return this.url;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getEndpoint() {
        return this.endpoint;
    }

    public void setParameter(String parameter) {
        this.parameter = parameter;
    }

    public String getParameter() {
        return this.parameter;
    }
}
