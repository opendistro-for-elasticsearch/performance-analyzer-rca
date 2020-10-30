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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.collator.validator;

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.HeapSizeIncreaseAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CollatorValidator implements IValidator {

  private static final Logger LOG = LogManager.getLogger(CollatorValidator.class);
  protected AppContext appContext;
  protected long startTime;

  public CollatorValidator() {
    this.appContext = new AppContext();
    this.startTime = System.currentTimeMillis();
  }

  @Override
  public boolean checkDbObj(Object object) {
    if (object == null) {
      return false;
    }

    PersistedAction persistedAction = (PersistedAction) object;
    // In this case, we expect pressure-mismatched actions to be suggested by the deciders. I.e.
    // JvmDecider asking to reduce heap pressure and QueueHealthDecider asking to increase heap
    // pressure. We expect the collator to have picked the pressure decreasing action which is
    // the HeapSizeIncreaseAction.

    assertEquals(HeapSizeIncreaseAction.NAME, persistedAction.getActionName());
    return true;
  }

}
