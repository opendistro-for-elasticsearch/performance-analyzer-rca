/*
 *  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval;

/** List of stats that are currently supported. */
public enum Statistics {
  MAX,
  MIN,
  MEAN,
  COUNT,
  SUM,

  // Samples are not aggregated. They are reported as they were found. They can be used when we
  // need just a key value pairs.
  SAMPLE,

  // Think of them as a counter per name. So if you update your stats as these values:
  // x, y, x, x, z, h
  // then the named counter will give you something like:
  // x: 3, y: 1, z: 1, h:1
  // This is helpful in calculating metric like which rca nodes threw exceptions and count per
  // graph node.
  NAMED_COUNTERS
}
