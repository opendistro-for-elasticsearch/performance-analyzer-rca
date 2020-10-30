package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.collator.validator;

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.HeapSizeIncreaseAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;

public class CollatorAlignedValidator extends CollatorValidator {

  @Override
  protected boolean checkPersistedAction(PersistedAction persistedAction) {
    assertEquals(HeapSizeIncreaseAction.NAME, persistedAction.getActionName());
    return true;
  }
}
