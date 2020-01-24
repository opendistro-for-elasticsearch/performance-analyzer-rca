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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.format;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.vals.AggregateValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.vals.NamedAggregateValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.vals.Value;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultFormatter implements Formatter {
  private Map<MeasurementSet, Map<Statistics, List<Value>>> map;
  private long start;
  private long end;

  public DefaultFormatter() {
    this.map = new HashMap<>();
    this.start = 0;
    this.end = 0;
  }

  @Override
  public void formatNamedAggregatedValue(
      MeasurementSet measurementSet, Statistics aggregationType, String name, Number value) {
    map.putIfAbsent(measurementSet, new HashMap<>());
    map.get(measurementSet).putIfAbsent(aggregationType, new ArrayList<>());
    map.get(measurementSet)
        .get(aggregationType)
        .add(new NamedAggregateValue(value, aggregationType, name));
  }

  @Override
  public void formatAggregatedValue(
      MeasurementSet measurementSet, Statistics aggregationType, Number value) {
    map.putIfAbsent(measurementSet, new HashMap<>());
    Value value1;
    if (aggregationType == Statistics.SAMPLE) {
      value1 = new Value(value);
    } else {
      value1 = new AggregateValue(value, aggregationType);
    }

    map.get(measurementSet).put(aggregationType,
            Collections.singletonList(value1));
  }

  @Override
  public void setStartAndEndTime(long start, long end) {
    this.start = start;
    this.end = end;
  }

  public Map<MeasurementSet, Map<Statistics, List<Value>>> getFormatted() {
    return map;
  }
}
