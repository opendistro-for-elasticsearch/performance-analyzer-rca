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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.MalformedThresholdFile;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.OverridesAndPrecedenceOrderCountMismatch;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.OverridesAndPrecedenceOrderValueMismatch;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Threshold {
  private final String name;
  private String pathToFile;
  private final String defaultVal;
  private long creationTime;
  private List<String> overridePrecedenceOrder;
  private Map<String, Map<String, String>> overrides;
  private boolean creationTimeModifiedOnce;
  private boolean pathToFileModifiedOnce;

  public String getName() {
    return name;
  }

  Threshold(
      @JsonProperty("name") String name,
      @JsonProperty("default") String defaultVal,
      @JsonProperty("overrides") Map<String, Map<String, String>> overrides,
      @JsonProperty("precedence-order") List<String> overridePrecedenceOrder) {
    this.name = name;
    this.defaultVal = defaultVal;
    this.creationTime = 0;
    this.overridePrecedenceOrder = overridePrecedenceOrder;
    this.overrides = overrides;
    this.pathToFile = null;
    this.pathToFileModifiedOnce = false;
    this.creationTimeModifiedOnce = false;
  }

  /**
   * Run through the overrides in the order of the precedence-list and return the first match or the
   * default otherwise.
   *
   * @return value of the threshold.
   */
  public String get(RcaConf rcaConf) {
    // In the order as defined in the precedence order, we go through the overrides map, and this
    // gives us all
    // the override category: domain, instance-type etc.
    for (String overrideCategory : overridePrecedenceOrder) {
      Map<String, String> inCategoryMap = overrides.get(overrideCategory);
      // Get the overrideCategory's value from the RcaConf's tags. With that value, we check if
      // there is an
      // override available. If so, we return, otherwise, we inspect the next override category.
      String val = inCategoryMap.get(rcaConf.getTagMap().get(overrideCategory));
      if (val != null) {
        // The value of this category in the rca.conf, might not match any of the provided
        // overrides. So,
        // this check is important.
        return val;
      }
    }

    // If we are here, it means that no override condition matched and so we go ahead with the
    // default value.
    return defaultVal;
  }

  String getPathToFile() {
    return pathToFile;
  }

  void setPathToFile(String pathToFile) {
    if (pathToFileModifiedOnce) {
      throw new RuntimeException("Path to the threshold can be set only once.");
    }
    this.pathToFile = pathToFile;
    pathToFileModifiedOnce = true;
  }

  void setCreationTime(long creationTime) {
    if (creationTimeModifiedOnce) {
      throw new RuntimeException("Creation time of a threshold can be modified only once.");
    }
    this.creationTime = creationTime;
    creationTimeModifiedOnce = true;
  }

  long getCreationTime() {
    return creationTime;
  }

  void validate() throws MalformedThresholdFile {
    if (name == null || defaultVal == null) {
      throw new MalformedThresholdFile(pathToFile, "'name' and 'default' keys are mandatory.");
    }

    String filename = new File(pathToFile).getName();
    if (!filename.split("\\.")[0].equals(name)) {
      throw new MalformedThresholdFile(
          pathToFile,
          "The name of the file (without the extension)"
              + " and the value of the 'name' key in json, should match.");
    }

    if (overrides != null && overridePrecedenceOrder != null) {
      if (overrides.size() != overridePrecedenceOrder.size()) {
        String msg = "Elements in overrides and precedence-order are not the same.";
        throw new OverridesAndPrecedenceOrderCountMismatch(pathToFile, msg);
      }
      // At this point we validateAndProcess that the all the categories of overrides, are present
      // in the precedence-order.
      for (String cat : overridePrecedenceOrder) {
        if (overrides.get(cat) == null) {
          String msg =
              String.format("'%s' exists in the precedence-order but not in the overrides", cat);
          throw new OverridesAndPrecedenceOrderValueMismatch(pathToFile, msg);
        }
      }
    } else {
      // If we are here then either overrides is null or overridesPrecedenceOrder is null or both
      // are null. If both are null, then its a valid case, else we throw error.
      if (!(overrides == null && overridePrecedenceOrder == null)) {
        String msg = "Between overrides and precedence-order, either you specify both or none.";
        throw new OverridesAndPrecedenceOrderValueMismatch(pathToFile, msg);
      } else {
        // We don't like nulls and null pointer exceptions. So we will initialize these with isEmpty
        // maps and
        // lists.
        overrides = new HashMap<>();
        overridePrecedenceOrder = new ArrayList<>();
      }
    }
  }
}
