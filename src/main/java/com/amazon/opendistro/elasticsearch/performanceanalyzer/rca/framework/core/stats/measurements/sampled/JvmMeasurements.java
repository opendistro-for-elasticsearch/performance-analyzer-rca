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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.sampled;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.MeasurementSet;
import java.util.Collections;
import java.util.List;

/**
 * These are the group of measurements that are sampled every so often and then they are reported.
 * They are not aggregated.
 */
public enum JvmMeasurements implements MeasurementSet {
  JVM_FREE_MEM_SAMPLER("JvmFreeMem", "bytes"),
  JVM_TOTAL_MEM_SAMPLER("JvmTotalMem", "bytes"),
  THREAD_COUNT("ThreadCount", "count");

  private String name;
  private String unit;

  JvmMeasurements(String name, String unit) {
    this.name = name;
    this.unit = unit;
  }

  @Override
  public List<Statistics> getStatsList() {
    return Collections.singletonList(Statistics.SAMPLE);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getUnit() {
    return unit;
  }

  @Override
  public String toString() {
    return new StringBuilder(name).append("-").append(unit).toString();
  }
}
