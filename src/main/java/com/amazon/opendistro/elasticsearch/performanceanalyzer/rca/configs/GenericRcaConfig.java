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

import java.util.Map;

/**
 * base class config object to store rca config settings
 */
public abstract class GenericRcaConfig {

  /**
   * read the rca name defined in rca.conf
   * @return rca name
   */
  public abstract String getRcaName();

  /**
   * read the Map object entry of the specific rca from the rcaConfigSettings
   * @param rcaConfigSettings the Map object pointing to "rca-config-settings" in rca.conf
   * @return the Map object entry of the specific rca
   * @throws ClassCastException throw exception if fails to casting the object to Map object
   */
  @SuppressWarnings("unchecked")
  protected Map<String, Object> getRcaMapObject(Map<String, Object> rcaConfigSettings) throws ClassCastException {
    Map<String, Object> ret = null;
    if (rcaConfigSettings != null
        && rcaConfigSettings.containsKey(getRcaName())
        && rcaConfigSettings.get(getRcaName()) != null) {
      ret = (Map<String, Object>)rcaConfigSettings.get(getRcaName());
    }
    return ret;
  }
}
