package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.collator.validator;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;

public class CollatorMisalignedValidator extends CollatorValidator {

  @Override
  protected boolean checkPersistedAction(PersistedAction persistedAction) {
    return false;
  }
}
