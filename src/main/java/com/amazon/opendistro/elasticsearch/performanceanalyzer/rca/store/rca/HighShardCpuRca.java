package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.INDEX_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.OPERATION;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.SHARD_ID;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.SHARD_ROLE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.MovingAverage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.FlowUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HighShardCpuRca extends Rca {
  private static final Logger LOG = LogManager.getLogger(HighShardCpuRca.class);
  private Map<String, MovingAverage> averageMap;

  public HighShardCpuRca(long evaluationIntervalMins) {
    super(evaluationIntervalMins);
    averageMap = new HashMap<>();
  }

  /**
   * If X% of the samples is above Y%, then we deem it to be a high-cpu in this symptom.
   *
   * @param dependencies The data from the upstream nodes
   * @return
   */
  @Override
  public FlowUnit operate(Map<Class, FlowUnit> dependencies) {
    // The operate here tries to find hot shard. The way it tries to do it is creates a
    // MovingAverage object for
    // each distinct shardID. The moving average window is defined as three samples for less that
    // three it
    // outputs -1 and for greater than or equal to 3, it outputs the average for that window. This
    // method
    // compares it against an arbitrary threshold of 90% and if such a shard exists, then it reports
    // it as a flow
    // unit. So the symptom can be defined as if there exists a shard for which the average of max
    // is greater
    // than 90% for three consecutive samples, then classify it as hot. This is not accurate in
    // production but it
    // tests the system works for arbitrary samples of data.
    FlowUnit cpuMetric = dependencies.get(CPU_Utilization.class);

    List<List<String>> allData = cpuMetric.getData();
    List<String> cols = allData.get(0);
    int shardIDIdx = -1;
    int maxColIdx = -1;

    List<String> dims =
        new ArrayList<String>() {
          {
            this.add(SHARD_ID.toString());
            this.add(INDEX_NAME.toString());
            this.add(OPERATION.toString());
            this.add(SHARD_ROLE.toString());
          }
        };
    // Get the index of the shardID column.
    for (int i = 0; i < cols.size(); i++) {
      if (cols.get(i).equals(dims.get(0))) {
        shardIDIdx = i;
        break;
      }
    }

    // Get the index of the max column.
    for (int i = 0; i < cols.size(); i++) {
      if (cols.get(i).equals(MetricsDB.MAX)) {
        maxColIdx = i;
        break;
      }
    }

    final double HIGH_CPU_THRESHOLD = 90.0;
    List<List<String>> ret = new ArrayList<>();
    Map<String, String> context = new HashMap<>();

    // The first row is the column names, so we start from the row 1.
    for (int i = 1; i < allData.size(); i++) {
      List<String> row = allData.get(i);
      String shardId = row.get(shardIDIdx);
      MovingAverage entry = averageMap.get(shardId);
      if (null == entry) {
        entry = new MovingAverage(3);
        averageMap.put(shardId, entry);
      }
      double val = entry.next(Double.parseDouble(row.get(maxColIdx)));
      if (val > HIGH_CPU_THRESHOLD) {
        List<String> dataRow = Collections.singletonList(shardId);
        context.put("threshold", String.valueOf(HIGH_CPU_THRESHOLD));
        context.put("actual", String.valueOf(val));
        ret.add(dataRow);
        LOG.info(
            String.format(
                "Shard %s is hot. Average max CPU (%f) above: %f",
                shardId, val, HIGH_CPU_THRESHOLD));
      }
    }
    return new FlowUnit(System.currentTimeMillis(), ret, context);
  }
}
