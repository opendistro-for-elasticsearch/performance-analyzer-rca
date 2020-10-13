package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvmsizing.validator;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;

public abstract class HeapSizeIncreaseValidator implements IValidator {

  AppContext appContext;
  long startTime;

  public HeapSizeIncreaseValidator() {
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

  public abstract boolean checkPersistedAction(final PersistedAction persistedAction);
}
