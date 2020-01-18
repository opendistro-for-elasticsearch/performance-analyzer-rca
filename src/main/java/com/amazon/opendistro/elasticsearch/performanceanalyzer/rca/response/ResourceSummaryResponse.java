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

public class ResourceSummaryResponse {
  private String resourceName;
  private String unitType;
  private Double thresholdValue;
  private Double actualValue;
  private Double averageValue;
  private Double minimumValue;
  private Double maximumValue;


  public ResourceSummaryResponse(String resourceName,
                                 String unitType,
                                 Double thresholdValue,
                                 Double actualValue,
                                 Double averageValue,
                                 Double minimumValue,
                                 Double maximumValue) {
    this.resourceName = resourceName;
    this.unitType = unitType;
    this.thresholdValue = thresholdValue;
    this.actualValue = actualValue;
    this.averageValue = averageValue;
    this.minimumValue = minimumValue;
    this.maximumValue = maximumValue;
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getUnitType() {
    return unitType;
  }

  public Double getThresholdValue() {
    return thresholdValue;
  }

  public Double getActualValue() {
    return actualValue;
  }

  public Double getAverageValue() {
    return averageValue;
  }

  public Double getMinimumValue() {
    return minimumValue;
  }

  public Double getMaximumValue() {
    return maximumValue;
  }

  @Override
  public String toString() {
    return "{"
            + "\"ResourceType\" : \"" + resourceName + "\","
            + "\"UnitType\" : \"" + unitType + "\","
            + "\"Threshold\" : " + thresholdValue + ","
            + "\"Actual\" : " + actualValue + ","
            + "\"Average\" : " + averageValue + ","
            + "\"Minimum\" : " + minimumValue + ","
            + "\"Maximum\" : " + maximumValue
            + '}';
  }
}
