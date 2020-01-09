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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** This is the basic unit of transfer between the nodes. */
public class FlowUnit {
  private long timeStamp;

  // The first row is the name of the columns.
  // The values are second row and onwards.
  private List<List<String>> data;
  private Map<String, String> contextMap;
  private boolean empty = true;

  public FlowUnit(long timeStamp, List<List<String>> data, Map<String, String> contextMap) {
    this.timeStamp = timeStamp;
    this.data = data;
    this.contextMap = contextMap;
    empty = false;
  }

  // Creates an empty flow unit.
  public FlowUnit(long timeStamp) {
    this.timeStamp = timeStamp;
    this.data = Collections.emptyList();
    this.contextMap = Collections.emptyMap();
  }

  public List<List<String>> getData() {
    return data;
  }

  public Map<String, String> getContextMap() {
    return contextMap;
  }

  public String getContextString() {
    return contextMap.toString();
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public static FlowUnit generic() {
    return new FlowUnit(System.currentTimeMillis());
  }

  public boolean isEmpty() {
    return empty;
  }

  @Override
  public String toString() {
    return String.format("%d: %s :: context %s", timeStamp, data, contextMap);
  }
}
