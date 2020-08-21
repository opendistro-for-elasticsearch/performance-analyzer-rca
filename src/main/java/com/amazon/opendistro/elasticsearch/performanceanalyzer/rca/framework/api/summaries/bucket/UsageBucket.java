/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket;

/**
 * A UsageBucket is associated with a {@link com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource}
 * and identifies the state of that Resource. We use these buckets to identify when we have the
 * bandwidth to scale a particular resource out or in.
 *
 * <p>{@link UsageBucket#HEALTHY_WITH_BUFFER} means that the
 * {@link com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource} is healthy and
 * potentially under utilized. Resources in this state are good candidates for scaling in
 *
 * <p>{@link UsageBucket#HEALTHY} means that the
 * {@link com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource} is in a healthy
 * state. Resources in this bucket should probably be left alone.
 *
 * <p>{@link UsageBucket#UNDER_UTILIZED} means that the
 * {@link com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource} is on the verge of
 * being unhealthy. The utilization is high, but not quite high enough to be called unhealthy yet.
 * Resources in this bucket are good candidates for scaling out.
 *
 * <p>{@link UsageBucket#UNHEALTHY} means that the
 * {@link com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource} is under high
 * pressure. Actions should be taken to help reduce the pressure.
 *
 */
public enum UsageBucket {
  UNKNOWN,
  UNDER_UTILIZED,
  HEALTHY_WITH_BUFFER,
  HEALTHY,
  UNHEALTHY
}
