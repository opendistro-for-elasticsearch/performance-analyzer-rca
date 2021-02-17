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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class GarbageCollectorInfo {

  private static final Map<String, Supplier<String>> gcSuppliers;
  private static final ImmutableMap<String, String> memoryPoolMap;

  private static final String SURVIVOR = "Survivor";
  private static final String EDEN = "Eden";
  private static final String OLD_GEN = "OldGen";
  private static final String PERM_GEN = "PermGen";

  static {
    gcSuppliers = new HashMap<>();
    memoryPoolMap = ImmutableMap.<String, String>builder()
        // Perm gen region names as read by different collectors.
        .put("CMS Perm Gen", PERM_GEN)
        .put("Perm Gen", PERM_GEN)
        .put("PS Perm Gen", PERM_GEN)
        .put("G1 Perm Gen", PERM_GEN)
        .put("Metaspace", PERM_GEN)
        // Old gen region names as read by different collectors.
        .put("CMS Old Gen", OLD_GEN)
        .put("Tenured Gen", OLD_GEN)
        .put("PS Old Gen", OLD_GEN)
        .put("G1 Old Gen", OLD_GEN)
        // Young gen region names as read by different collectors.
        .put("Par Eden Space", EDEN)
        .put("Eden Space", EDEN)
        .put("PS Eden Space", EDEN)
        .put("G1 Eden", EDEN)
        // Survivor space as read by different collectors.
        .put("Par Survivor Space", SURVIVOR)
        .put("Survivor Space", SURVIVOR)
        .put("PS Survivor Space", SURVIVOR)
        .put("G1 Survivor", SURVIVOR)
        .build();

    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

    for (GarbageCollectorMXBean gcBean : gcBeans) {
      String[] memoryPools = gcBean.getMemoryPoolNames();
      if (memoryPools != null && memoryPools.length > 0) {
        for (String memoryPool : memoryPools) {
          String genericMemoryPool = memoryPoolMap.getOrDefault(memoryPool, memoryPool);
          gcSuppliers.putIfAbsent(genericMemoryPool, gcBean::getName);
        }
      }
    }
  }

  public static Map<String, Supplier<String>> getGcSuppliers() {
    return gcSuppliers;
  }

  @VisibleForTesting
  public static ImmutableMap<String, String> getMemoryPoolMap() {
    return memoryPoolMap;
  }
}
