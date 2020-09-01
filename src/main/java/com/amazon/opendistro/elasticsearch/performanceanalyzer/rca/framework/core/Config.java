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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import java.util.Map;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Config<T> {

  private static final Logger LOG = LogManager.getLogger(Config.class);

  private String key;
  private T value;

  public Config(String key, @Nullable Map<String, Object> parentConfig, T defaultValue, Class<? extends T> clazz) {
    this(key, parentConfig, defaultValue, (s) -> true, clazz);
  }

  public Config(String key, @Nullable Map<String, Object> parentConfig, T defaultValue, Predicate<T> validator, Class<? extends T> clazz) {
    this.key = key;
    this.value = defaultValue;
    if (parentConfig != null) {
      try {
        T configValue = clazz.cast(parentConfig.getOrDefault(key, defaultValue));
        if (!validator.test(configValue)) {
          LOG.error("Config value: [{}] provided for key: [{}] is invalid", configValue, key);
        } else {
          value = configValue;
        }
      } catch (ClassCastException e) {
        LOG.error("rca.conf contains invalid value for key: [{}], trace : [{}]", key, e.getMessage());
      }
    }
  }

  public String getKey() {
    return key;
  }

  public T getValue() {
    return value;
  }
}
