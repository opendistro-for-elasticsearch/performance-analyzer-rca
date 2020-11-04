package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.framework.api;

import com.google.gson.JsonElement;

/**
 * This interface specifies a Validator that can used for validation of results. Based on what is required to be validated
 * the RCA-IT framework will call one of the check methods with the latest data from the current iteration.
 */
public interface IValidator {

  /**
   * Based on what is required to be validated,
   *
   * @param response The REST response returns JSONElement.
   * @return By default, return false. Implementations returns true, if this matches expectation.
   */
  default boolean checkJsonResp(JsonElement response) {
    return false;
  }

  /**
   * Based on what is required to be validated,
   *
   * @param object On querying the db returns an object of the required class.
   * @return By default, return false. Implementations returns true, if this matches expectation.
   */
  default boolean checkDbObj(Object object) {
    return false;
  }
}
