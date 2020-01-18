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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.NamedAggregateValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.Value;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.MeasurementSet;
import java.util.HashMap;
import java.util.Map;

public class BaseFormatter implements Formatter {
  private Map<MeasurementSet, Map<Statistics, Value>> map;
  private long start;
  private long end;

  public BaseFormatter() {
    this.map = new HashMap<>();
    this.start = 0;
    this.end = 0;
  }

  @Override
  public void formatNamedAggregatedValue(
      MeasurementSet measurementSet, Statistics aggregationType, String name, Number value) {
    map.putIfAbsent(measurementSet, new HashMap<>());
    map.get(measurementSet)
        .put(aggregationType, new NamedAggregateValue(value, aggregationType, name));
  }

  @Override
  public void formatAggregatedValue(
      MeasurementSet measurementSet, Statistics aggregationType, Number value) {
    map.putIfAbsent(measurementSet, new HashMap<>());
    map.get(measurementSet).put(aggregationType, new Value(value));
  }

  @Override
  public void setStartAndEndTime(long start, long end) {
    this.start = start;
    this.end = end;
  }

  public Map<MeasurementSet, Map<Statistics, Value>> getFormatted() {
    return map;
  }
}
