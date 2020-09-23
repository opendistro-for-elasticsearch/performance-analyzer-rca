package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api;

import com.google.gson.JsonElement;
import org.jooq.Record;
import org.jooq.Result;

/**
 * This interface specifies a Validator that can used for validation of results. Based on what is required to be validated
 * the RCA-IT framework will call one of the check methods with the latest data from the current iteration.
 */
public interface IValidator {

  /**
   * Based on what is required to be validated,
   *
   * @param response The REST response returns JSONElement and sqlite response returns </T> (class type).
   * @return true, if this matches expectation.
   */
  <T> boolean check(T response);
}
