/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature;

/**
 * Given the units of the resource consumed and the total consumption and
 */
public class HeatZoneAssigner {

  public static Zone assign(final TemperatureVector.NormalizedValue consumed,
      final TemperatureVector.NormalizedValue nodeAvg,
      final TemperatureVector.NormalizedValue threshold) {
    Zone zone;
    if (consumed.isGreaterThan(nodeAvg)) {
      TemperatureVector.NormalizedValue deviation = consumed.diff(nodeAvg);
      if (deviation.isGreaterThan(threshold)) {
        zone = Zone.HOT;
      } else {
        zone = Zone.WARM;
      }
    } else {
      TemperatureVector.NormalizedValue deviation = nodeAvg.diff(consumed);
      if (deviation.isGreaterThan(threshold)) {
        zone = Zone.COLD;
      } else {
        zone = Zone.LUKE_WARM;
      }
    }
    return zone;
  }

  public enum Zone {
    HOT,
    WARM,
    LUKE_WARM,
    COLD
  }
}
