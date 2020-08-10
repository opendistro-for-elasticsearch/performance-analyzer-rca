package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.Cluster;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.HostTag;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.jooq.Record;
import org.jooq.Result;

/**
 * This is API class whose object is injected into each of the test methods in case test class
 * declares a @{code setTestApi(final TestApi api)}.
 */
public class TestApi {
  /**
   * An instance of the cluster object to get access to the nodes to query various data to see
   * that the tests have the desired results.
   */
  private final Cluster cluster;

  public TestApi(Cluster cluster) {
    this.cluster = cluster;
  }

  public JsonElement getRcaDataOnHost(HostTag hostTag, String rcaName) {
    return cluster.getAllRcaDataOnHost(hostTag, rcaName);
  }

  /**
   * This let's you make a REST request to the REST endpoint of a particular host identified by
   * the host tag.
   *
   * @param params    the key value map that is passes as the request parameter.
   * @param hostByTag The host whose rest endpoint we will hit.
   * @return The response serialized as a String.
   */
  public String getRcaRestResponse(@Nonnull final Map<String, String> params,
                                   HostTag hostByTag) {
    Objects.requireNonNull(params);
    return cluster.getRcaRestResponse(params, hostByTag);
  }
}
