package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard.IndexShardKey;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Result;

/**
 * We've seen huge performance impact if collecting node stats across all shards on data node.
 * So Performance Analyzer writer will only try to collect node stats from a small portion of
 * shards at a time to reduce performance impact. This class on reader side will allow us to
 * keep track of node stat from all previous batches and calculate its sum.
 */
public class NodeStatAggregator {

  private static final Logger LOG = LogManager.getLogger(NodeStatAggregator.class);
  private Metric nodeStatMetric;
  private int sum;
  private final HashMap<IndexShardKey, NodeStatValue> shardKeyMap;
  private long lastPurgeTimestamp;
  //purge the hash table every 30 mins
  private static final int PURGE_HASH_TABLE_INTERVAL_IN_MINS = 30;


  public NodeStatAggregator(Metric nodeStatMetric) {
    this.nodeStatMetric = nodeStatMetric;
    this.sum = 0;
    this.lastPurgeTimestamp = 0L;
    this.shardKeyMap = new HashMap<>();
  }

  /**
   * Whether this NodeStatAggregator contains valid node stats from writer
   * This is to avoid reading stale data when the node stats has already been
   * disabled from writer side
   * @return if it has valid node stats
   */
  public boolean isEmpty() {
    return shardKeyMap.isEmpty();
  }

  /**
   * get the name of node stat metric.
   * i.e. Norms_Memory, Cache_FieldData_Size, etc.
   * @return name of node stat metric
   */
  public String getName() {
    return nodeStatMetric.name();
  }

  /**
   * get the sum of node stat metric across all shards on this node
   * @return sum of node stat metric
   */
  public int getSum() {
    return this.sum;
  }

  /**
   * call this method to collect node stats of each shard from node stat metric
   * and calculate its sum.
   * @param timestamp current timestamp when collecting from metricDB
   */
  public void collect(final long timestamp) {
    for (MetricFlowUnit metric : nodeStatMetric.getFlowUnits()) {
      if (metric.isEmpty()) {
        continue;
      }
      Result<Record> result = metric.getData();
      for (Record record : result) {
        try {
          IndexShardKey shardKey = IndexShardKey.buildIndexShardKey(record);
          Integer value = record.getValue(MetricsDB.MAX, Integer.class);
          NodeStatValue oldNodeStatValue = shardKeyMap.getOrDefault(shardKey, new NodeStatValue(0, 0));
          shardKeyMap.put(shardKey, new NodeStatValue(value, timestamp));
          this.sum += (value - oldNodeStatValue.getValue());
        }
        catch (Exception e) {
          LOG.error("Fail to read parse node stats {}", this.getName());
        }
      }
    }
    //purge the hashtable
    if (TimeUnit.MILLISECONDS.toMinutes(timestamp - this.lastPurgeTimestamp) > PURGE_HASH_TABLE_INTERVAL_IN_MINS) {
      purgeHashTable(timestamp);
    }
  }

  // shards can be deleted from ES while still remains in this hashtable
  // or we might disable the Node Stats collector on writer to stop sending node stats to reader
  // in either case, we need to write a function to clean up this hashtable on reader periodically
  // to remove node stats of inactive shards
  private void purgeHashTable(final long timestamp) {
    Iterator<IndexShardKey> iterator = this.shardKeyMap.keySet().iterator();
    while (iterator.hasNext()) {
      IndexShardKey key = iterator.next();
      long timestampDiff = timestamp - this.shardKeyMap.get(key).getTimestamp();
      if (TimeUnit.MILLISECONDS.toMinutes(timestampDiff) > PURGE_HASH_TABLE_INTERVAL_IN_MINS) {
        this.sum -= this.shardKeyMap.get(key).getValue();
        iterator.remove();
      }
    }
  }

  private static class NodeStatValue {
    private int value;
    private long timestamp;

    public NodeStatValue(int value, long timestamp) {
      this.value = value;
      this.timestamp = timestamp;
    }

    public int getValue() {
      return this.value;
    }

    public long getTimestamp() {
      return this.timestamp;
    }
  }
}
