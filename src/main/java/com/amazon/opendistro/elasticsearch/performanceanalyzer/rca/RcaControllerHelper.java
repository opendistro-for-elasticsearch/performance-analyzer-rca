package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.AESRcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaRuntimeMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RcaControllerHelper {

  private static final Logger LOG = LogManager.getLogger(RcaControllerHelper.class);
  public static final String CAT_MASTER_URL = "http://localhost:9200/_cat/master?h=ip";
  private static String ELECTED_MASTER_RCA_CONF_PATH = RcaConsts.RCA_CONF_MASTER_PATH;
  private static String MASTER_RCA_CONF_PATH = RcaConsts.RCA_CONF_IDLE_MASTER_PATH;
  private static String RCA_CONF_PATH = RcaConsts.RCA_CONF_PATH;

  /**
   * Picks a configuration for RCA based on the node's role.
   *
   * @param nodeRole The role of the node(data/eligible master/elected master)
   * @return The configuration based on the role.
   */
  public static RcaConf pickRcaConfForRole(final NodeRole nodeRole) {
    if (NodeRole.ELECTED_MASTER == nodeRole) {
      LOG.debug("picking elected master conf");
      return Util.IS_AES ? new AESRcaConf(ELECTED_MASTER_RCA_CONF_PATH) : new RcaConf(ELECTED_MASTER_RCA_CONF_PATH);
    }

    if (NodeRole.MASTER == nodeRole) {
      LOG.debug("picking idle master conf");
      return Util.IS_AES ? new AESRcaConf(MASTER_RCA_CONF_PATH) : new RcaConf(MASTER_RCA_CONF_PATH);
    }

    if (NodeRole.DATA == nodeRole) {
      LOG.debug("picking data node conf");
      return Util.IS_AES ? new AESRcaConf(RCA_CONF_PATH) : new RcaConf(RCA_CONF_PATH);
    }

    LOG.debug("picking default conf");
    return Util.IS_AES ? new AESRcaConf(RCA_CONF_PATH) : new RcaConf(RCA_CONF_PATH);
  }

  /**
   * Gets the elected master's information by performing a _cat/master call.
   *
   * @return The host address of the elected master.
   */
  public static String getElectedMasterHostAddress() {
    try {
      LOG.info("Making _cat/master call");
      PerformanceAnalyzerApp.RCA_RUNTIME_METRICS_AGGREGATOR.updateStat(
          RcaRuntimeMetrics.ES_APIS_CALLED, "catMaster", 1);

      final URL url = new URL(CAT_MASTER_URL);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String inputLine = in.readLine();
      in.close();

      return inputLine;
    } catch (IOException e) {
      LOG.error("Could not get the elected master node: {}", e.getMessage());
      e.printStackTrace();
    }

    return "";
  }

  /**
   * Builds a thread pool used by the networking layer to pass messages and perform networking
   * functions.
   *
   * @param queueLength The length of the queue in the threadpool.
   * @return The thread pool as an executor service.
   */
  public static ExecutorService buildNetworkThreadPool(final int queueLength) {
    final ThreadFactory rcaNetThreadFactory =
        new ThreadFactoryBuilder().setNameFormat(RcaConsts.RCA_NETWORK_THREAD_NAME_FORMAT)
                                  .setDaemon(true)
                                  .build();
    final BlockingQueue<Runnable> threadPoolQueue = new LinkedBlockingQueue<>(queueLength);
    return new ThreadPoolExecutor(RcaConsts.NETWORK_CORE_THREAD_COUNT,
        RcaConsts.NETWORK_MAX_THREAD_COUNT, 0L, TimeUnit.MILLISECONDS, threadPoolQueue,
        rcaNetThreadFactory);
  }

  public static void set(final String rcaConfPath, final String rcaMaterConfPath,
      final String rcaElectedMasterConfPath) {
    RCA_CONF_PATH = rcaConfPath;
    MASTER_RCA_CONF_PATH = rcaMaterConfPath;
    ELECTED_MASTER_RCA_CONF_PATH = rcaElectedMasterConfPath;
  }

  public static List<String> getAllConfFilePaths() {
    return ImmutableList.of(ELECTED_MASTER_RCA_CONF_PATH, MASTER_RCA_CONF_PATH, RCA_CONF_PATH);
  }
}
