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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.format;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;

/**
 * A formatter is used to get the final formatted output of an aggregation.
 *
 * <p>The idea is without having to know the internal structure of the aggregator, one should be
 * able to get a formatted result out. When the caller, passes on a formatter to get(), the get
 * internally calls formatNamedValue and formatValue multiple times one for each statistic value per
 * aggregated Measurement. Say, if you are measuring min, max, mean for X and min and max for Y,
 * then internally formatNamedValue or formatvalue will be called five times in total; 3 times for
 * min, max and mean for X and 2 times for min and max.
 *
 * <p>It is upto the implementing class to store the values however it wants.
 */
public interface Formatter {

  /**
   * This function that knows what to do with named value types or knows how to store them.
   *
   * @param measurementSet The name of the measurement.
   * @param aggregationType whether this is min, max, mean or other statistic.
   * @param name The name of the value.
   * @param value The value of the value.
   */
  void formatNamedAggregatedValue(
      MeasurementSet measurementSet, Statistics aggregationType, String name, Number value);

  /**
   * This knows how to store the value, when called with one.
   *
   * @param measurementSet The name of the measurement.
   * @param aggregationType The name of aggregation type - min, max and the like.
   * @param value The value of the measurement, corresponding to the aggregation type.
   */
  void formatAggregatedValue(
      MeasurementSet measurementSet, Statistics aggregationType, Number value);

  /**
   * This method is called by the PerRunStats to set the start and end time of that particular run.
   *
   * @param start The time when the first metric came in.
   * @param end The time when the getAndReset was called on the PerRunMetric
   */
  void setStartAndEndTime(long start, long end);
}
