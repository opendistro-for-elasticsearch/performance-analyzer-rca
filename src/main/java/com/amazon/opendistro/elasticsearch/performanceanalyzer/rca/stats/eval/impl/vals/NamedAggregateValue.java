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

public class NamedAggregateValue extends AggregateValue {
  private String name;

  public NamedAggregateValue(Number value, Statistics type, String name) {
    super(value, type);
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public void format(Formatter formatter, MeasurementSet measurementSet, Statistics stats) {
    formatter.formatNamedAggregatedValue(
        measurementSet, getAggregationType(), getName(), getValue());
  }

  public void update(Number value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    NamedAggregateValue that = (NamedAggregateValue) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name);
  }

  @Override
  public String toString() {
    return "NamedAggregateValue{"
        + "name='"
        + name
        + '\''
        + ", aggr='"
        + getAggregationType()
        + '\''
        + ", value="
        + value
        + '}';
  }
}
