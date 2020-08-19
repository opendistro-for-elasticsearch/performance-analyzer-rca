package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConfigUtils {
  private static final Logger LOG = LogManager.getLogger(ConfigUtils.class);

  public static <T> T readConfig(Map<String, Object> config, String key, Class<? extends T> clazz) {
    T setting = null;
    try {
      if (config != null
          && config.containsKey(key)
          && config.get(key) != null) {
        setting = clazz.cast(config.get(key));
      }
    } catch (ClassCastException ne) {
      LOG.error("rca.conf contains value in invalid format, trace : {}", ne.getMessage());
    }
    return setting;
  }
}
