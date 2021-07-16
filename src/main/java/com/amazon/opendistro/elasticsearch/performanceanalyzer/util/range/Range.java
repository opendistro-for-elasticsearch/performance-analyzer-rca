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


import java.util.Objects;

public class Range {

    private double lowerBound;
    private double upperBound;
    private double threshold;

    public Range(double lowerBound, double upperBound, double threshold) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.threshold = threshold;
    }

    public boolean contains(double value) {
        return value >= lowerBound && value <= upperBound;
    }

    public double getLowerBound() {
        return lowerBound;
    }

    public double getUpperBound() {
        return upperBound;
    }

    public double getThreshold() {
        return threshold;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Range range = (Range) o;
        return Double.compare(range.getLowerBound(), getLowerBound()) == 0
                && Double.compare(range.getUpperBound(), getUpperBound()) == 0
                && Double.compare(range.getThreshold(), getThreshold()) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getLowerBound(), getUpperBound(), getThreshold());
    }

    @Override
    public String toString() {
        return String.format(
                "Range{lowerBound=%s, upperBound=%s, threshold=%s}",
                lowerBound, upperBound, threshold);
    }
}
