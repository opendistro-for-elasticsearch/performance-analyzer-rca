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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response;

import java.util.Objects;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * ResourceSummaryResponse contains information such as the name of the hot resource, the current
 * value threshold, maximum etc.
 */
public class ResourceSummaryResponse {
  private String resourceName;
  private String unitType;
  private Double threshold;
  private Double actual;
  private Double average;
  private Double minimum;
  private Double maximum;

  public ResourceSummaryResponse(
      String resourceName,
      String unitType,
      Double threshold,
      Double actual,
      Double average,
      Double minimum,
      Double maximum) {
    this.resourceName = resourceName;
    this.unitType = unitType;
    this.threshold = threshold;
    this.actual = actual;
    this.average = average;
    this.minimum = minimum;
    this.maximum = maximum;
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getUnitType() {
    return unitType;
  }

  public Double getThresholdValue() {
    return threshold;
  }

  public Double getActualValue() {
    return actual;
  }

  public Double getAverageValue() {
    return average;
  }

  public Double getMinimumValue() {
    return minimum;
  }

  public Double getMaximumValue() {
    return maximum;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ResourceSummaryResponse that = (ResourceSummaryResponse) o;
    return Objects.equals(resourceName, that.resourceName)
        && Objects.equals(unitType, that.unitType)
        && Objects.equals(threshold, that.threshold)
        && Objects.equals(actual, that.actual)
        && Objects.equals(average, that.average)
        && Objects.equals(minimum, that.minimum)
        && Objects.equals(maximum, that.maximum);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(resourceName)
        .append(unitType)
        .append(threshold)
        .append(actual)
        .append(average)
        .append(minimum)
        .append(maximum)
        .toHashCode();
  }
}
