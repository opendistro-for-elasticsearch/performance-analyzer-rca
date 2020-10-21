/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvmsizing.validator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.HeapSizeIncreaseAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import java.util.concurrent.TimeUnit;

public class HeapSizeIncreaseValidatorColocatedMaster extends HeapSizeIncreaseValidator {

  @Override
  public boolean checkPersistedAction(final PersistedAction persistedAction) {
    assertTrue(persistedAction.isActionable());
    assertFalse(persistedAction.isMuted());
    assertEquals(HeapSizeIncreaseAction.NAME, persistedAction.getActionName());
    assertEquals(TimeUnit.DAYS.toMillis(3), persistedAction.getCoolOffPeriod());
    assertEquals("{DATA_0}", persistedAction.getNodeIds());
    assertEquals("{127.0.0.1}", persistedAction.getNodeIps());
    return true;
  }
}
