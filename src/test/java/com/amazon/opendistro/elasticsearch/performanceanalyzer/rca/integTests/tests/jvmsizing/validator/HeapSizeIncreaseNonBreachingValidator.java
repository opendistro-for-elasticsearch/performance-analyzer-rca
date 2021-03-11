package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvmsizing.validator;

import static org.junit.Assert.assertNotEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.HeapSizeIncreaseAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;

public class HeapSizeIncreaseNonBreachingValidator extends HeapSizeIncreaseValidator {

  @Override
  public boolean checkDbObj(Object object) {
    // It could well be the case that no RCA has been triggered so far, and thus no table exists.
    // This is a valid outcome.
    if (object == null) {
      return true;
    }

    PersistedAction persistedAction = (PersistedAction) object;
    return checkPersistedAction(persistedAction);
  }

  @Override
  public boolean checkPersistedAction(PersistedAction persistedAction) {
    assertNotEquals(HeapSizeIncreaseAction.NAME, persistedAction.getActionName());

    return true;
  }
}
