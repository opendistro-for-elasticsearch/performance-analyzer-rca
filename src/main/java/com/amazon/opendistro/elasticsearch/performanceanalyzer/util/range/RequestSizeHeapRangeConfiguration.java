/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.util.range;


import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** RequestSize controller threshold based on heap occupancy percent range */
public class RequestSizeHeapRangeConfiguration implements RangeConfiguration {

    private static Collection<Range> rangeConfiguration = defaultRangeConfiguration();

    @Override
    public Range getRange(double value) {
        return rangeConfiguration.stream()
                .filter(range -> range.contains(value))
                .findFirst()
                .orElse(null);
    }

    @Override
    public boolean hasRangeChanged(double previousValue, double currentValue) {
        Range previousRange = getRange(previousValue);
        Range currentRange = getRange(currentValue);
        if (previousRange == null && currentRange == null) {
            return false;
        }
        if (previousRange == null || currentRange == null) {
            return true;
        }
        return !previousRange.equals(currentRange);
    }

    @Override
    public void setRangeConfiguration(Collection<Range> rangeConfiguration) {
        RequestSizeHeapRangeConfiguration.rangeConfiguration = rangeConfiguration;
    }

    /**
     * @return list of request-size controller threshold for lower and upper bound of heap percent
     *     new Range(0, 75, 15.0) => for heap percent between 0% and 75% set threshold to 15%
     */
    private static List<Range> defaultRangeConfiguration() {
        return Collections.unmodifiableList(
                Arrays.asList(
                        new Range(0, 75, 15.0),
                        new Range(76, 80, 12.5),
                        new Range(81, 85, 10.0),
                        new Range(86, 90, 7.5),
                        new Range(91, 95, 5.0),
                        new Range(96, 100, 2.5)));
    }
}
