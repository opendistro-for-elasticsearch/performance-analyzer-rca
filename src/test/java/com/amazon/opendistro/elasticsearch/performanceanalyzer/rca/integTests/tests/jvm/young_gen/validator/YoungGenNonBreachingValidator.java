package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvm.young_gen.validator;

import static org.junit.Assert.assertNotEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.CacheClearAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.HeapSizeIncreaseAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.JvmGenAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;

public class YoungGenNonBreachingValidator implements IValidator {
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

  private boolean checkPersistedAction(PersistedAction persistedAction) {
    //validate no old gen action is emitted
//    assertNotEquals(ModifyCacheMaxSizeAction.NAME, persistedAction.getActionName());
//    assertNotEquals(ModifyQueueCapacityAction.NAME, persistedAction.getActionName());
//    assertNotEquals(CacheClearAction.NAME, persistedAction.getActionName());

    //validate no heapSizeIncreaseAction is emitted
    assertNotEquals(HeapSizeIncreaseAction.NAME, persistedAction.getActionName());

    //validate no young gen action is emitted
    assertNotEquals(JvmGenAction.NAME, persistedAction.getActionName());
    return true;
  }
}
