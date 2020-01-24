/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.vals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.format.Formatter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;
import java.util.Objects;

public class AggregateValue extends Value {
  private Statistics aggregationType;

  public AggregateValue(Number value, Statistics type) {
    super(value);
    this.aggregationType = type;
  }

  public void format(Formatter formatter, MeasurementSet measurementSet, Statistics stats) {
    formatter.formatAggregatedValue(measurementSet, stats, value);
  }

  public Statistics getAggregationType() {
    return aggregationType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    AggregateValue that = (AggregateValue) o;
    return aggregationType == that.aggregationType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), aggregationType);
  }

  @Override
  public String toString() {
    return "AggregateValue{" + "aggregationType=" + aggregationType + ", value=" + value + '}';
  }
}
