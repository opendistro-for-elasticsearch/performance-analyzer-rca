package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.JsonConverter;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class ConfigOverridesWrapperTests {
    private ConfigOverridesWrapper testConfigOverridesWrapper;
    private final ConfigOverrides validTestOverrides = buildValidConfigOverrides();
    private final String validTestOverrideJson = JsonConverter
        .writeValueAsString(validTestOverrides);

    @Mock
    private ObjectMapper mockMapper;

    public ConfigOverridesWrapperTests() throws JsonProcessingException {
    }

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        testConfigOverridesWrapper = new ConfigOverridesWrapper();
        testConfigOverridesWrapper.setCurrentClusterConfigOverrides(validTestOverrides);
    }

    @Test
    public void testSerializeSuccess() throws IOException {
        String serializedOverrides = testConfigOverridesWrapper.serialize(validTestOverrides);

        assertEquals(validTestOverrideJson, serializedOverrides);
    }

    @Test
    public void testDeserializeSuccess() throws IOException {
        ConfigOverrides deserializedOverrides = testConfigOverridesWrapper.deserialize(validTestOverrideJson);

        assertEquals(validTestOverrides.getEnable().getRcas(), deserializedOverrides.getEnable().getRcas());
        assertEquals(validTestOverrides.getEnable().getDeciders(), deserializedOverrides.getEnable().getDeciders());
        assertEquals(validTestOverrides.getEnable().getActions(), deserializedOverrides.getEnable().getActions());

        assertEquals(validTestOverrides.getDisable().getRcas(), deserializedOverrides.getDisable().getRcas());
        assertEquals(validTestOverrides.getDisable().getDeciders(), deserializedOverrides.getDisable().getDeciders());
        assertEquals(validTestOverrides.getDisable().getActions(), deserializedOverrides.getDisable().getActions());
    }

    @Test(expected = IOException.class)
    public void testSerializeInvalidOverrides() throws IOException {
        when(mockMapper.writeValueAsString(any())).thenThrow(JsonParseException.class);

        testConfigOverridesWrapper = new ConfigOverridesWrapper(mockMapper);
        testConfigOverridesWrapper.serialize(new ConfigOverrides());
    }

    @Test(expected = IOException.class)
    public void testDeserializeNullString() throws IOException {
        testConfigOverridesWrapper.deserialize("invalid json");
    }

    private ConfigOverrides buildValidConfigOverrides() {
        ConfigOverrides overrides = new ConfigOverrides();
        overrides.getDisable().setRcas(Arrays.asList("rca1", "rca2"));
        overrides.getDisable().setActions(Arrays.asList("action1", "action2"));
        overrides.getEnable().setRcas(Arrays.asList("rca3", "rca4"));
        overrides.getEnable().setDeciders(Collections.singletonList("decider1"));

        return overrides;
    }
}