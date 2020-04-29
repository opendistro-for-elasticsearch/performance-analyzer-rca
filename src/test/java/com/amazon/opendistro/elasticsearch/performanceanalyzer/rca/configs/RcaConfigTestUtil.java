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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs;

import java.util.HashMap;
import java.util.Map;

public class RcaConfigTestUtil {

  @SuppressWarnings("unchecked")
  public static void putToRcaMap(Map<String, Object> settings, String rcaName, String key, Object value) {
    if (!settings.containsKey(rcaName)) {
      settings.put(rcaName, new HashMap<String, Object>());
    }
    Map<String, Object> rcaMapObj = (Map<String, Object>) settings.get(rcaName);
    rcaMapObj.put(key, value);
  }

  @SuppressWarnings("unchecked")
  public static void clearRcaMap(Map<String, Object> settings, String rcaName) {
    if (settings.containsKey(rcaName)) {
      Map<String, Object> rcaMapObj = (Map<String, Object>) settings.get(rcaName);
      rcaMapObj.clear();
    }
  }
}
