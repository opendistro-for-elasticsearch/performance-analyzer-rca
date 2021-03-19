/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators;

import java.util.Iterator;
import java.util.Objects;

public class SlidingWindowTestUtil {
  public static boolean equals(SlidingWindow<SlidingWindowData> a, SlidingWindow<SlidingWindowData> b) {
    Objects.requireNonNull(a);
    Objects.requireNonNull(b);
    if (a.size() != b.size()) {
      return false;
    }
    Iterator<SlidingWindowData> aIt = a.windowDeque.descendingIterator();
    Iterator<SlidingWindowData> bIt = b.windowDeque.descendingIterator();
    while (aIt.hasNext()) {
      SlidingWindowData aData = aIt.next();
      SlidingWindowData bData = bIt.next();
      if (aData.getValue() != bData.getValue() || aData.getTimeStamp() != bData.getTimeStamp()) {
        return false;
      }
    }
    return true;
  }

  public static boolean equals_MUTATE(SlidingWindow<SlidingWindowData> a, SlidingWindow<SlidingWindowData> b) {
    Objects.requireNonNull(a);
    Objects.requireNonNull(b);
    if (a.size() != b.size()) {
      return false;
    }
    while (a.size() > 0) {
      if (a.windowDeque.getLast().getValue() != b.windowDeque.getLast().getValue()) {
        return false;
      }
      a.windowDeque.removeLast();
      b.windowDeque.removeLast();
    }
    return true;
  }
}
