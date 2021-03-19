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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api;

import com.google.gson.JsonElement;

/**
 * This interface specifies a Validator that can used for validation of results. Based on what is required to be validated
 * the RCA-IT framework will call one of the check methods with the latest data from the current iteration.
 */
public interface IValidator {

  /**
   * Based on what is required to be validated,
   *
   * @param response The REST response returns JSONElement.
   * @return By default, return false. Implementations returns true, if this matches expectation.
   */
  default boolean checkJsonResp(JsonElement response) {
    return false;
  }

  /**
   * Based on what is required to be validated,
   *
   * @param object On querying the db returns an object of the required class.
   * @return By default, return false. Implementations returns true, if this matches expectation.
   */
  default boolean checkDbObj(Object object) {
    return false;
  }
}
