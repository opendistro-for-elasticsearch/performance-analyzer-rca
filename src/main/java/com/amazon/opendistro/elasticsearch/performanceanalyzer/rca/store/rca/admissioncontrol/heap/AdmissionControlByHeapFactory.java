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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admissioncontrol.heap;


import java.util.HashMap;
import java.util.Map;

/** HeapRcaFactory returns HeapRca based on maxHeap in gigabytes */
public class AdmissionControlByHeapFactory {

    // Heap Size in Gigabytes
    private static final int SMALL_HEAP_RCA_THRESHOLD = 4;
    private static final int MEDIUM_HEAP_RCA_THRESHOLD = 32;

    // Keys for rcaMap
    private static final String SMALL_HEAP = "SMALL_HEAP";
    private static final String MEDIUM_HEAP = "MEDIUM_HEAP";
    private static final String LARGE_HEAP = "LARGE_HEAP";

    private static final Map<String, AdmissionControlByHeap> rcaMap = new HashMap<>();

    public static AdmissionControlByHeap getByMaxHeap(double maxHeap) {
        if (maxHeap <= SMALL_HEAP_RCA_THRESHOLD) {
            if (!rcaMap.containsKey(SMALL_HEAP))
                rcaMap.put(SMALL_HEAP, new AdmissionControlBySmallHeap());
            return rcaMap.get(SMALL_HEAP);
        } else if (maxHeap <= MEDIUM_HEAP_RCA_THRESHOLD) {
            if (!rcaMap.containsKey(MEDIUM_HEAP))
                rcaMap.put(MEDIUM_HEAP, new AdmissionControlByMediumHeap());
            return rcaMap.get(MEDIUM_HEAP);
        } else {
            if (!rcaMap.containsKey(LARGE_HEAP))
                rcaMap.put(LARGE_HEAP, new AdmissionControlByLargeHeap());
            return rcaMap.get(LARGE_HEAP);
        }
    }
}
