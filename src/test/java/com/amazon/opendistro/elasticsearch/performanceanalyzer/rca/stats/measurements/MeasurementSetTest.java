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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import java.util.Arrays;
import java.util.List;

public enum MeasurementSetTest implements MeasurementSet {
  TEST_MEASUREMENT1(
      "TestMeasurement1", "micros", Arrays.asList(Statistics.MAX, Statistics.MEAN, Statistics.MIN)),
  TEST_MEASUREMENT2("TestMeasurement2", "micros", Arrays.asList(Statistics.COUNT)),
  TEST_MEASUREMENT3("TestMeasurement3", "micros", Arrays.asList(Statistics.COUNT)),
  TEST_MEASUREMENT4("TestMeasurement4", "micros", Arrays.asList(Statistics.SAMPLE)),
  TEST_MEASUREMENT5("TestMeasurement5", "micros", Arrays.asList(Statistics.SUM)),
  TEST_MEASUREMENT6("TestMeasurement6", "micros", Arrays.asList(Statistics.NAMED_COUNTERS)),
  JVM_FREE_MEM_SAMPLER("jvmFreeMemorySampler", "bytes", Arrays.asList(Statistics.SAMPLE));

  private String name;
  private String unit;
  private List<Statistics> statsList;

  MeasurementSetTest(String name, String unit, List<Statistics> statisticList) {
    this.name = name;
    this.unit = unit;
    this.statsList = statisticList;
  }

  public String toString() {
    return new StringBuilder(name).append("-").append(unit).toString();
  }

  @Override
  public List<Statistics> getStatsList() {
    return statsList;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getUnit() {
    return unit;
  }
}
