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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.helpers;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

public class AssertHelper {
  public static <T extends Comparable> void compareLists(List<T> list1, List<T> list2) {
    assertEquals(list1.size(), list2.size());
    for (int i = 0; i < list1.size(); i++) {
      assertEquals(list1.get(i), list2.get(i));
    }
  }

  public static <K extends Comparable, V extends Comparable> void compareMaps(
      Map<K, V> map1, Map<K, V> map2) {
    assertEquals(map1.size(), map2.size());
    for (Map.Entry<K, V> entry : map1.entrySet()) {
      K key = entry.getKey();
      V value = entry.getValue();
      V value2 = map2.get(key);

      if (value instanceof String) {
        double vd1 = Double.parseDouble((String) value);
        double vd2 = Double.parseDouble((String) value2);
        assertEquals(vd1, vd2, 0.1);
      } else {
        assertEquals(value, value2);
      }
    }
  }
}
