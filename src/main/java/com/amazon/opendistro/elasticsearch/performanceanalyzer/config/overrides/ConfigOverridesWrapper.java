package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Class responsible for holding the latest config overrides across the cluster.
 */
public class ConfigOverridesWrapper {

    private volatile ConfigOverrides currentClusterConfigOverrides;
    private volatile long lastUpdatedTimestamp;
    private final ObjectMapper mapper;

    public ConfigOverridesWrapper() {
        this(new ObjectMapper());
    }

    /**
     * Ctor used only for unit test purposes.
     * @param mapper The object mapper instance.
     */
    public ConfigOverridesWrapper(final ObjectMapper mapper) {
        this.currentClusterConfigOverrides = new ConfigOverrides();
        this.mapper = mapper;
    }

    public ConfigOverrides getCurrentClusterConfigOverrides() {
        return currentClusterConfigOverrides;
    }

    /**
     * Sets a new ConfigOverrides instance as the current cluster config overrides instance.
     *
     * @param configOverrides the ConfigOverrides instance.
     */
    public void setCurrentClusterConfigOverrides(final ConfigOverrides configOverrides) {
        this.currentClusterConfigOverrides = configOverrides;
    }

    /**
     * Deserializes a JSON representation of the config overrides into a {@link ConfigOverrides} instance.
     *
     * @param overrides The JSON string representing config overrides.
     * @return A {@link ConfigOverrides} instance if the JSON is valid.
     * @throws IOException if conversion runs into an IOException.
     */
    public ConfigOverrides deserialize(final String overrides) throws IOException {
        final IOException[] exception = new IOException[1];
        final ConfigOverrides configOverrides = AccessController.doPrivileged((PrivilegedAction<ConfigOverrides>) () -> {
            try {
                return mapper.readValue(overrides, ConfigOverrides.class);
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

    /**
     * Serializes a {@link ConfigOverrides} instance to its JSON representation.
     *
     * @param overrides The {@link ConfigOverrides} instance.
     * @return String in JSON format representing the serialized equivalent.
     * @throws IOException if conversion runs into an IOException.
     */
    public String serialize(final ConfigOverrides overrides) throws IOException {
        final IOException[] exception = new IOException[1];
        final String serializedOverrides = AccessController.doPrivileged((PrivilegedAction<String>) () -> {
            try {
                return mapper.writeValueAsString(overrides);
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

    public long getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(long lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }
}
