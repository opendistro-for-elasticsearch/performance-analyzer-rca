package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Class that helps with operations concerning {@link ConfigOverrides}s
 */
public class ConfigOverridesHelper {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Serializes a {@link ConfigOverrides} instance to its JSON representation.
   *
   * @param overrides The {@link ConfigOverrides} instance.
   * @return String in JSON format representing the serialized equivalent.
   * @throws IOException if conversion runs into an IOException.
   */
  public static String serialize(final ConfigOverrides overrides) throws IOException {
    final IOException[] exception = new IOException[1];
    final String serializedOverrides = AccessController.doPrivileged((PrivilegedAction<String>) () -> {
      try {
        return MAPPER.writeValueAsString(overrides);
      } catch (IOException e) {
        exception[0] = e;
      }
      return "";
    });

    if (serializedOverrides.isEmpty() && exception[0] != null) {
      throw exception[0];
    }

    return serializedOverrides;
  }

  /**
   * Deserializes a JSON representation of the config overrides into a {@link ConfigOverrides} instance.
   *
   * @param overrides The JSON string representing config overrides.
   * @return A {@link ConfigOverrides} instance if the JSON is valid.
   * @throws IOException if conversion runs into an IOException.
   */
  public static ConfigOverrides deserialize(final String overrides) throws IOException {
    final IOException[] exception = new IOException[1];
    final ConfigOverrides configOverrides = AccessController.doPrivileged((PrivilegedAction<ConfigOverrides>) () -> {
      try {
        return MAPPER.readValue(overrides, ConfigOverrides.class);
      } catch (IOException ioe) {
        exception[0] = ioe;
      }
      return null;
    });

    if (configOverrides == null && exception[0] != null) {
      // re throw the exception that was consumed while deserializing.
      throw exception[0];
    }

    return configOverrides;
  }
}