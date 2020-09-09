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
   * @param response This REST response as a JSONElement.
   * @return true, if this matches expectation.
   */
  <T> boolean check(T response);
}
