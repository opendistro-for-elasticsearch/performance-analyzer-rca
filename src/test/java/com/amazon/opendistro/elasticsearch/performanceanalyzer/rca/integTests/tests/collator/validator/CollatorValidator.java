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
