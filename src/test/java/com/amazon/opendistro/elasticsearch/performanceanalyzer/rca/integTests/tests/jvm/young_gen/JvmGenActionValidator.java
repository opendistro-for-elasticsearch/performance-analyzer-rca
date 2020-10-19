package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvm.young_gen;

import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.JvmGenAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;

public class JvmGenActionValidator implements IValidator {
  AppContext appContext;
  long startTime;

  public JvmGenActionValidator() {
    appContext = new AppContext();
    startTime = System.currentTimeMillis();
  }

  @Override
  public boolean checkDbObj(Object object) {
    if (object == null) {
      return false;
    }

    PersistedAction persistedAction = (PersistedAction) object;
    return checkPersistedAction(persistedAction);
  }

  private boolean checkPersistedAction(final PersistedAction persistedAction) {
    JvmGenAction heapSizeIncreaseAction =
        JvmGenAction.fromSummary(persistedAction.getSummary(), appContext);
    assertTrue(heapSizeIncreaseAction.getTargetRatio() <= 5);
    return true;
  }
}
