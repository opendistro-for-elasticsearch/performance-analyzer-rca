package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.collator.validator;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class CollatorValidator implements IValidator {

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
    return checkPersistedAction(persistedAction);
  }

  @Override
  public boolean checkJsonResp(JsonElement response) {
    if (response == null) {
      return false;
    }
    JsonArray arr = response.getAsJsonObject().get("data").getAsJsonArray();
    if (arr.size() > 0) {
      LOG.error("kak: {}", arr);
      return true;
    }
    return false;
  }

  protected abstract boolean checkPersistedAction(final PersistedAction persistedAction);
}
