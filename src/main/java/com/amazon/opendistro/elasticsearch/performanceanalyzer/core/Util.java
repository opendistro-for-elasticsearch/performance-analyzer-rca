package com.amazon.opendistro.elasticsearch.performanceanalyzer.core;

import java.io.File;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

public class Util {
  private static final Logger LOG = LogManager.getLogger(Util.class);
  public static final String METRICS_QUERY_URL = "/_opendistro/_performanceanalyzer/metrics";
  public static final String RCA_QUERY_URL = "/_opendistro/_performanceanalyzer/rca";
  public static final String PLUGIN_LOCATION =
      System.getProperty("es.path.home")
          + File.separator
          + "plugins"
          + File.separator
          + "opendistro_performance_analyzer"
          + File.separator;
  public static final String READER_LOCATION =
      System.getProperty("es.path.home")
          + File.separator
          + "opendistro_performance_analyzer"
          + File.separator;
  public static final String DATA_DIR =
      System.getProperty("es.path.home")
          + File.separator
          + "var"
          + File.separator
          + "es"
          + File.separator
          + "data"
          + File.separator;

  public static void invokePrivileged(Runnable runner) {
    AccessController.doPrivileged(
        (PrivilegedAction<Void>)
            () -> {
              try {
                runner.run();
              } catch (Exception ex) {
                LOG.debug(
                    (Supplier<?>)
                        () ->
                            new ParameterizedMessage(
                                "Privileged Invocation failed {}", ex.toString()),
                    ex);
              }
              return null;
            });
  }

  public static void invokePrivilegedAndLogError(Runnable runner) {
    AccessController.doPrivileged(
        (PrivilegedAction<Void>)
            () -> {
              try {
                runner.run();
              } catch (Exception ex) {
                LOG.error(
                    (Supplier<?>)
                        () ->
                            new ParameterizedMessage(
                                "Privileged Invocation failed {}", ex.toString()),
                    ex);
              }
              return null;
            });
  }
}
