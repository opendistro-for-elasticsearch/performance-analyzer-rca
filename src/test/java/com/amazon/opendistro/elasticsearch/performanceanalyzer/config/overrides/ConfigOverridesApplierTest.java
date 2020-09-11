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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverrides.Overrides;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

@Ignore
public class ConfigOverridesApplierTest {

  private static final String RCA1 = "rca1";
  private static final String RCA3 = "rca3";
  private static final String RCA2 = "rca2";

  private static final String DEC1 = "dec1";
  private static final String DEC2 = "dec2";

  private static final String ACT1 = "act1";
  private static final String ACT2 = "act2";

  private List<String> initialMutedRcas = Collections.singletonList(RCA2);
  private List<String> initialMutedDeciders = Collections.singletonList(DEC1);
  private List<String> initialMutedActions = Collections.singletonList(ACT1);

  private ConfigOverridesApplier testOverridesApplier;
  private ConfigOverrides testOverrides;


  @Mock
  private RcaController mockRcaController;

  @Mock
  private RcaConf mockRcaConf;

  @Captor
  ArgumentCaptor<Set<String>> newMutedRcas;

  @Captor
  ArgumentCaptor<Set<String>> newMutedDeciders;

  @Captor
  ArgumentCaptor<Set<String>> newMutedActions;

  @Before
  public void setup() {
    initMocks(this);
    this.testOverridesApplier = new ConfigOverridesApplier();
    this.testOverrides = buildTestOverrides();
    when(mockRcaController.getRcaConf()).thenReturn(mockRcaConf);
    when(mockRcaController.isRcaEnabled()).thenReturn(true);
    when(mockRcaConf.getMutedRcaList()).thenReturn(initialMutedRcas);
    when(mockRcaConf.getMutedDeciderList()).thenReturn(initialMutedDeciders);
    when(mockRcaConf.getMutedActionList()).thenReturn(initialMutedActions);

    PerformanceAnalyzerApp.setRcaController(mockRcaController);
  }

  @Test
  public void testInvalidOverridesOrTimestamp() {
    testOverridesApplier.applyOverride(null, null);
    assertEquals(0, testOverridesApplier.getLastAppliedTimestamp());

    testOverridesApplier.applyOverride("", "");
    assertEquals(0, testOverridesApplier.getLastAppliedTimestamp());

    testOverridesApplier.applyOverride("{}", "not a number");
    assertEquals(0, testOverridesApplier.getLastAppliedTimestamp());
  }

  @Test
  public void testTimestampInThePast() {
    long lastAppliedTimestamp = System.currentTimeMillis();
    testOverridesApplier.setLastAppliedTimestamp(lastAppliedTimestamp);
    testOverridesApplier.applyOverride("{}",
        Long.toString(Instant.now().minusSeconds(300).getEpochSecond()));

    assertEquals(lastAppliedTimestamp, testOverridesApplier.getLastAppliedTimestamp());
  }

  @Test
  public void testRcaControllerNotSet() {
    PerformanceAnalyzerApp.setRcaController(null);

    long lastAppliedTimestamp = System.currentTimeMillis();
    testOverridesApplier.setLastAppliedTimestamp(lastAppliedTimestamp);

    testOverridesApplier.applyOverride("{}", Long.toString(lastAppliedTimestamp + 300));

    assertEquals(lastAppliedTimestamp, testOverridesApplier.getLastAppliedTimestamp());
  }

  @Test
  public void testRcaNotEnabled() {
    when(mockRcaController.isRcaEnabled()).thenReturn(false);

    long lastAppliedTimestamp = System.currentTimeMillis();
    testOverridesApplier.setLastAppliedTimestamp(lastAppliedTimestamp);

    testOverridesApplier.applyOverride("{}", Long.toString(lastAppliedTimestamp + 300));

    assertEquals(lastAppliedTimestamp, testOverridesApplier.getLastAppliedTimestamp());
  }

  @Test
  public void testApplyValidOverrides() throws JsonProcessingException {
    long lastAppliedTimestamp = System.currentTimeMillis();
    testOverridesApplier.setLastAppliedTimestamp(lastAppliedTimestamp);

    String overridesJson = new ObjectMapper().writeValueAsString(buildTestOverrides());

    testOverridesApplier.applyOverride(overridesJson, Long.toString(lastAppliedTimestamp + 300));

    verify(mockRcaConf).updateAllRcaConfFiles(newMutedRcas.capture(), newMutedDeciders.capture(),
        newMutedActions.capture());

    // Previously only rca2 was muted, now we disabled rca1, and rca3 as well.
    Set<String> combinedMutedRcas = newMutedRcas.getValue();
    assertEquals(3, combinedMutedRcas.size());
    assertEquals(ImmutableSet.of(RCA1, RCA2, RCA3), combinedMutedRcas);

    // Previously dec1 was muted, now we disabled dec1, and dec2.
    Set<String> combinedMutedDeciders = newMutedDeciders.getValue();
    assertEquals(2, combinedMutedDeciders.size());
    assertEquals(ImmutableSet.of(DEC1, DEC2), combinedMutedDeciders);

    // Previously act1 was disabled, now we enable act1, and disable act2.
    Set<String> combinedMutedActions = newMutedActions.getValue();
    assertEquals(1, combinedMutedActions.size());
    assertEquals(ImmutableSet.of(ACT2), combinedMutedActions);
  }

  @Test
  public void testUpdateRcaConfFailure() throws IOException {
    long lastAppliedTimestamp = System.currentTimeMillis();
    testOverridesApplier.setLastAppliedTimestamp(lastAppliedTimestamp);
    String overridesJson = new ObjectMapper().writeValueAsString(buildTestOverrides());
    when(mockRcaConf.updateAllRcaConfFiles(any(), any(), any())).thenReturn(false);

    testOverridesApplier.applyOverride(overridesJson, Long.toString(lastAppliedTimestamp + 300));

    assertEquals(lastAppliedTimestamp, testOverridesApplier.getLastAppliedTimestamp());
  }

  private ConfigOverrides buildTestOverrides() {
    final ConfigOverrides overrides = new ConfigOverrides();
    Overrides disabled = new Overrides();
    disabled.setRcas(Arrays.asList(RCA1, RCA3));
    disabled.setDeciders(Arrays.asList(DEC1, DEC2));
    disabled.setActions(Collections.singletonList(ACT2));

    Overrides enabled = new Overrides();
    enabled.setActions(Collections.singletonList(ACT1));

    overrides.setDisable(disabled);
    overrides.setEnable(enabled);

    return overrides;
  }
}