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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admissioncontrol.model;

/** Represents used heap and max heap in gigabytes */
public class HeapMetric {
    private final double usedHeap;
    private final double maxHeap;

    public HeapMetric(double usedHeap, double maxHeap) {
        this.usedHeap = usedHeap;
        this.maxHeap = maxHeap;
    }

    public double getUsedHeap() {
        return usedHeap;
    }

    public double getMaxHeap() {
        return maxHeap;
    }

    public double getHeapPercent() {
        if (this.getMaxHeap() == 0) {
            return 0;
        }
        return 100 * this.getUsedHeap() / this.getMaxHeap();
    }

    public boolean hasValues() {
        return this.getUsedHeap() != 0 && this.getMaxHeap() != 0;
    }

    @Override
    public String toString() {
        return "HeapMetric{" + "usedHeap=" + usedHeap + ", maxHeap=" + maxHeap + '}';
    }
}
