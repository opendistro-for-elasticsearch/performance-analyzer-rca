package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.collectors.Collector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format.Formatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is meant to be the registry for all the stats that are collected by the Rca framework and
 * needs to be reported periodically by the {@code StatsCollector.collectMetrics}
 */
public class RcaStatsReporter {
  /** The list of collectors for which a report can be generated. */
  private static List<Collector> collectors = null;

  /** The index of the collector whose measurements will be reported on this iteration. */
  private static int idxOfCollectorToReport = 0;

  /**
   * The collectors are added by the RcaController and the get Next report is called by the
   * StatsCollector class. This is to make sure that a report is not asked for while the collectors
   * are still being added.
   */
  private static boolean ready = false;

  public RcaStatsReporter() {
    collectors = new ArrayList<>();
    idxOfCollectorToReport = 0;
  }

  /**
   * This method is not thread safe.
   *
   * @param collector The collector to be added to the list.
   */
  public void addCollector(Collector collector) {
    collectors.add(collector);
  }

  public void setReady() {
    ready = true;
  }

  /**
   * The caller is expected to call this method with a {@code new} formatter every time.
   * This is not thread-safe.
   *
   * <p>Each time this is called, this method sources a measurement type and formats it and sends
   * it.
   *
   * @param formatter The formatter to use to format the measurementSet
   * @return true if there are collectors left to be reported or false otherwise.
   */
  public static boolean getNextReport(Formatter formatter) {
    if (!ready || collectors == null || collectors.isEmpty()) {
      return false;
    }

    Collector collector = collectors.get(idxOfCollectorToReport);
    collector.fillValuesAndReset(formatter);
    boolean ret = true;

    idxOfCollectorToReport += 1;

    if (idxOfCollectorToReport == collectors.size()) {
      ret = false;
      idxOfCollectorToReport = 0;
    }
    return ret;
  }
}
