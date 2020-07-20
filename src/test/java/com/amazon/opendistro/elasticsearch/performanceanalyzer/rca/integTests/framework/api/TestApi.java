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

  /**
   * Returns the contents of all the RCA tables from all the nodes in the cluster in a json
   * format. This can be too much data based on how long the test was running for.
   *
   * @return A json array representing the table data from all the nodes.
   */
  public JsonArray getAllRcaData() {
    return cluster.getAllRcaData();
  }

  /**
   * Get all the data for a particular RCA by name.
   *
   * @param rcaName The name of the RCA for which the data is desired.
   * @return The RCA data in json format.
   */
  public JsonArray getDataForRca(String rcaName) {
    return cluster.getDataForRca(rcaName);
  }

  /**
   * Given a nodeTag, this hits the rest Endpoint for the httpServer on that host and gets all
   * the RCAs on that host in JSON format. Note that RCAs, other than the temperatureProfile, are
   * only accessible from the elected master and therefore, this will return error.
   *
   * @param hostTag The tag for the node we want to query.
   * @return A response Json object.
   */
  public JsonObject getAllRcaDataOnHost(HostTag hostTag) {
    return cluster.getAllRcaDataOnHost(hostTag);
  }

  public JsonElement getRcaDataOnHost(HostTag hostTag, String rcaName) {
    return cluster.getAllRcaDataOnHost(hostTag, rcaName);
  }

  /**
   * To get the result from all the RCA and summary tables across all nodes in the cluster.
   * The key to the map is the node
   *
   * @return a map of hostId and the table form data for that node.
   */
  public Map<HostTag, List<Result<Record>>> getRecordsForAllTables() {
    return cluster.getRecordsForAllTables();
  }

  /**
   * Get the data for all tables for a particular host identified by its hostTags
   *
   * @param hostTag The host-tag for the host.
   * @return All the data for that host.
   * @throws IllegalArgumentException if you provide the tag that does not have an associated
   *                                  host with it.
   */
  public List<Result<Record>> getRecordsForAllTables(HostTag hostTag) {
    return cluster.getRecordsForAllTables(hostTag);
  }

  /**
   * @param hostTag the tag for the host.
   * @param rcaName The data for the RCA which is asked for.
   * @return All rows from the rca table from the host.
   * @throws IllegalArgumentException if you provide tag that does not have a host associated
   *                                  with it.
   */
  public Result<Record> getRecordsForAllTables(HostTag hostTag, String rcaName) {
    return cluster.getRecordsForAllTables(hostTag, rcaName);
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
