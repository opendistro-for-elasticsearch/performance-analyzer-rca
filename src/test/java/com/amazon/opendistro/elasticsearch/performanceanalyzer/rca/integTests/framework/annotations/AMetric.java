/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation can be used to specify the metrics that will be poured onto the RCA Graph. Under the covers,
 * the framework will try to simulate a DBProvider that will respond with these metrics when the RCA Metrics
 * nodes query for one. This annotation lets you specify one or more metric tables, similar to the 5 second
 * metric snapshots of the metricsdb files, and also specify a table for one or a group of cluster hosts.
 */
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(value = AMetric.Metrics.class)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface AMetric {
  // The metric this data is emulating.
  Class name();

  // The names of the dimension columns that the metrics has. The dimensions can be found here:
  // https://opendistro.github.io/for-elasticsearch-docs/docs/pa/reference/
  String[] dimensionNames();

  // Specify one or more tables for the metric where each table belongs to a host or a group of hosts.
  ATable[] tables();

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD})
  @interface Metrics {
    AMetric[] value();
  }
}
