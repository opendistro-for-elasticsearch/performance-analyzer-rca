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
import java.util.List;

/** This is a marker interface to bring all measurement sets under one type. */
public interface MeasurementSet {
  /**
   * The statistics that should be calculated for this measurement
   *
   * @return The list of statistics to be calculated for this measurement.
   */
  List<Statistics> getStatsList();

  /**
   * The name of the measurement.
   *
   * @return The name of the measurement.
   */
  String getName();

  /**
   * The unit of measurement. This is not used for calculation but just for reference.
   *
   * @return The string representation of the unit.
   */
  String getUnit();
}
