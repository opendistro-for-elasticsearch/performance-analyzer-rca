/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.JooqFieldValue;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * Contract between reader and writer. Writer write using the same values of these enums as json
 * keys (See all MetricStatus's subclasses in
 * com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors), while reader creates db
 * tables using these keys as column names and extract values using these keys. You should make sure
 * the the field names in the MetricStatus's subclasses and enum names match. Also, when you change
 * anything, modify JsonKeyTest accordingly. We use camelCase instead of the usual capital case for
 * enum members because they have better readability for the above use cases.
 */
public class AllMetrics {
  // metric name (not complete, only metrics use the json format and contains
  // numeric values. Will add more when needed)
  public enum MetricName {
    CACHE_CONFIG,
    CIRCUIT_BREAKER,
    HEAP_METRICS,
    DISK_METRICS,
    TCP_METRICS,
    IP_METRICS,
    THREAD_POOL,
    SHARD_STATS,
    MASTER_PENDING,
    MOUNTED_PARTITION_METRICS,
    ADMISSION_CONTROL_METRICS,
    SHARD_INDEXING_PRESSURE
  }

  // we don't store node details as a metric on reader side database.  We
  // use the information as part of http response.
  public enum NodeDetailColumns {
    ID(Constants.ID_VALUE),
    HOST_ADDRESS(Constants.HOST_ADDRESS_VALUE),
    ROLE(Constants.ROLE_VALUE),
    IS_MASTER_NODE(Constants.IS_MASTER_NODE);


    private final String value;

    NodeDetailColumns(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String ID_VALUE = "ID";
      public static final String HOST_ADDRESS_VALUE = "HOST_ADDRESS";
      public static final String ROLE_VALUE = "ROLE";
      public static final String IS_MASTER_NODE = "IS_MASTER_NODE";
    }
  }

  /** Enumeration of the roles. */
  public enum NodeRole {
    MASTER(RoleConstants.MASTER),
    ELECTED_MASTER(RoleConstants.ELECTED_MASTER),
    DATA(RoleConstants.DATA),
    UNKNOWN(RoleConstants.UNKNOWN);

    private final String value;

    NodeRole(final String value) {
      this.value = value;
    }

    public String role() {
      return value;
    }

    public static class RoleConstants {
      public static final String MASTER = "MASTER";
      public static final String ELECTED_MASTER = "ELECTED_MASTER";
      public static final String DATA = "DATA";
      public static final String UNKNOWN = "UNKNOWN";
    }
  }

  // contents of metrics
  public enum GCType {
    TOT_YOUNG_GC(Constants.TOT_YOUNG_GC_VALUE),
    TOT_FULL_GC(Constants.TOT_FULL_GC_VALUE),
    SURVIVOR(Constants.SURVIVOR_VALUE),
    PERM_GEN(Constants.PERM_GEN_VALUE),
    OLD_GEN(Constants.OLD_GEN_VALUE),
    EDEN(Constants.EDEN_VALUE),
    NON_HEAP(Constants.NON_HEAP_VALUE),
    HEAP(Constants.HEAP_VALUE);

    private final String value;

    GCType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String TOT_YOUNG_GC_VALUE = "totYoungGC";
      public static final String TOT_FULL_GC_VALUE = "totFullGC";
      public static final String SURVIVOR_VALUE = "Survivor";
      public static final String PERM_GEN_VALUE = "PermGen";
      public static final String OLD_GEN_VALUE = "OldGen";
      public static final String EDEN_VALUE = "Eden";
      public static final String NON_HEAP_VALUE = "NonHeap";
      public static final String HEAP_VALUE = "Heap";
    }
  }

  /*
   * column names of Cache_MaxSize table
   * cache type | sum | avg | max | min |
   *
   * <p>Example:
   * Field Data Cache|26214400.0|26214400.0|26214400.0|26214400.0
   * Shard Request Cache|80181985.0|80181985.0|80181985.0|80181985.0
   */
  public enum CacheConfigDimension implements MetricDimension, JooqFieldValue {
    CACHE_TYPE(Constants.TYPE_VALUE);

    private final String value;

    CacheConfigDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    @Override
    public Field<String> getField() {
      return DSL.field(DSL.name(this.value), String.class);
    }

    @Override
    public String getName() {
      return value;
    }

    public static class Constants {
      public static final String TYPE_VALUE = "CacheType";
    }
  }

  public enum CacheConfigValue implements MetricValue {
    CACHE_MAX_SIZE(Constants.CACHE_MAX_SIZE_VALUE);

    private final String value;

    CacheConfigValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String CACHE_MAX_SIZE_VALUE = "Cache_MaxSize";
    }
  }

  //list of caches
  public enum CacheType {
    FIELD_DATA_CACHE(Constants.FIELD_DATA_CACHE_NAME),
    SHARD_REQUEST_CACHE(Constants.SHARD_REQUEST_CACHE_NAME),
    NODE_QUERY_CACHE(Constants.NODE_QUERY_CACHE_NAME);

    private final String value;

    CacheType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String FIELD_DATA_CACHE_NAME = "field_data_cache";
      public static final String SHARD_REQUEST_CACHE_NAME = "shard_request_cache";
      public static final String NODE_QUERY_CACHE_NAME = "node_query_cache";
    }
  }

  // column names of database table
  public enum CircuitBreakerDimension implements MetricDimension {
    CB_TYPE(Constants.TYPE_VALUE);

    private final String value;

    CircuitBreakerDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String TYPE_VALUE = "CBType";
    }
  }

  // cannot use limit as it is a keyword in sql
  public enum CircuitBreakerValue implements MetricValue {
    CB_ESTIMATED_SIZE(Constants.ESTIMATED_VALUE),
    CB_TRIPPED_EVENTS(Constants.TRIPPED_VALUE),
    CB_CONFIGURED_SIZE(Constants.LIMIT_CONFIGURED_VALUE);

    private final String value;

    CircuitBreakerValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String ESTIMATED_VALUE = "CB_EstimatedSize";

      public static final String TRIPPED_VALUE = "CB_TrippedEvents";

      public static final String LIMIT_CONFIGURED_VALUE = "CB_ConfiguredSize";
    }
  }

  public enum HeapDimension implements MetricDimension, JooqFieldValue {
    MEM_TYPE(Constants.TYPE_VALUE);

    private final String value;

    HeapDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    @Override
    public Field<String> getField() {
      return DSL.field(DSL.name(this.value), String.class);
    }

    @Override
    public String getName() {
      return value;
    }

    public static class Constants {
      public static final String TYPE_VALUE = "MemType";
    }
  }

  public enum HeapValue implements MetricValue {
    GC_COLLECTION_EVENT(Constants.COLLECTION_COUNT_VALUE),
    GC_COLLECTION_TIME(Constants.COLLECTION_TIME_VALUE),
    HEAP_COMMITTED(Constants.COMMITTED_VALUE),
    HEAP_INIT(Constants.INIT_VALUE),
    HEAP_MAX(Constants.MAX_VALUE),
    HEAP_USED(Constants.USED_VALUE);

    private final String value;

    HeapValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String COLLECTION_COUNT_VALUE = "GC_Collection_Event";

      public static final String COLLECTION_TIME_VALUE = "GC_Collection_Time";

      public static final String COMMITTED_VALUE = "Heap_Committed";

      public static final String INIT_VALUE = "Heap_Init";

      public static final String MAX_VALUE = "Heap_Max";

      public static final String USED_VALUE = "Heap_Used";
    }
  }

  public enum GCInfoDimension implements MetricDimension, JooqFieldValue {

    MEMORY_POOL(Constants.MEMORY_POOL_VALUE),
    COLLECTOR_NAME(Constants.COLLECTOR_NAME_VALUE);

    private final String value;

    GCInfoDimension(String value) {
      this.value = value;
    }

    @Override
    public String getName() {
      return value;
    }

    @Override
    public Field<String> getField() {
      return DSL.field(DSL.name(this.value), String.class);
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String MEMORY_POOL_VALUE = "MemoryPool";
      public static final String COLLECTOR_NAME_VALUE = "CollectorName";
    }
  }

  public enum GCInfoValue implements MetricValue {
    GARBAGE_COLLECTOR_TYPE(Constants.GC_TYPE_VALUE);

    private final String value;

    GCInfoValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String GC_TYPE_VALUE = "GC_Type";
    }
  }

  public enum DiskDimension implements MetricDimension {
    DISK_NAME(Constants.NAME_VALUE);

    private final String value;

    DiskDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String NAME_VALUE = "DiskName";
    }
  }

  public enum DiskValue implements MetricValue {
    DISK_UTILIZATION(Constants.UTIL_VALUE),
    DISK_WAITTIME(Constants.WAIT_VALUE),
    DISK_SERVICE_RATE(Constants.SRATE_VALUE);

    private final String value;

    DiskValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String UTIL_VALUE = "Disk_Utilization";

      public static final String WAIT_VALUE = "Disk_WaitTime";

      public static final String SRATE_VALUE = "Disk_ServiceRate";
    }
  }

  public enum DevicePartitionDimension implements MetricDimension {
    MOUNT_POINT(Constants.MOUNT_POINT_VALUE),
    DEVICE_PARTITION(Constants.DEVICE_PARTITION_VALUE);

    private final String value;

    DevicePartitionDimension(final String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    public static class Constants {

      public static final String MOUNT_POINT_VALUE = "MountPoint";
      public static final String DEVICE_PARTITION_VALUE = "DevicePartition";
    }
  }

  public enum DevicePartitionValue implements MetricValue {
    TOTAL_SPACE(Constants.TOTAL_SPACE_VALUE),
    FREE_SPACE(Constants.FREE_SPACE_VALUE),
    USABLE_FREE_SPACE(Constants.USABLE_FREE_SPACE_VALUE);

    private final String value;

    DevicePartitionValue(final String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    public static class Constants {

      public static final String TOTAL_SPACE_VALUE = "Partition_TotalSpace";
      public static final String FREE_SPACE_VALUE = "Partition_FreeSpace";
      public static final String USABLE_FREE_SPACE_VALUE = "Partition_UsableFreeSpace";
    }
  }

  public enum TCPDimension implements MetricDimension {
    DEST_ADDR(Constants.DEST_VALUE);

    private final String value;

    TCPDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String DEST_VALUE = "DestAddr";
    }
  }

  public enum TCPValue implements MetricValue {
    Net_TCP_NUM_FLOWS(Constants.NUM_FLOWS_VALUE),
    Net_TCP_TXQ(Constants.TXQ_VALUE),
    Net_TCP_RXQ(Constants.RXQ_VALUE),
    Net_TCP_LOST(Constants.CUR_LOST_VALUE),
    Net_TCP_SEND_CWND(Constants.SEND_CWND_VALUE),
    Net_TCP_SSTHRESH(Constants.SSTHRESH_VALUE);

    private final String value;

    TCPValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String NUM_FLOWS_VALUE = "Net_TCP_NumFlows";

      public static final String TXQ_VALUE = "Net_TCP_TxQ";

      public static final String RXQ_VALUE = "Net_TCP_RxQ";

      public static final String CUR_LOST_VALUE = "Net_TCP_Lost";

      public static final String SEND_CWND_VALUE = "Net_TCP_SendCWND";

      public static final String SSTHRESH_VALUE = "Net_TCP_SSThresh";
    }
  }

  public enum IPDimension implements MetricDimension {
    DIRECTION(Constants.DIRECTION_VALUE);

    private final String value;

    IPDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String DIRECTION_VALUE = "Direction";
    }
  }

  public enum IPValue implements MetricValue {
    NET_PACKET_RATE4(Constants.PACKET_RATE4_VALUE),
    NET_PACKET_DROP_RATE4(Constants.DROP_RATE4_VALUE),
    NET_PACKET_RATE6(Constants.PACKET_RATE6_VALUE),
    NET_PACKET_DROP_RATE6(Constants.DROP_RATE6_VALUE),
    NET_THROUGHPUT(Constants.THROUGHPUT_VALUE);

    private final String value;

    IPValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String PACKET_RATE4_VALUE = "Net_PacketRate4";
      public static final String DROP_RATE4_VALUE = "Net_PacketDropRate4";
      public static final String PACKET_RATE6_VALUE = "Net_PacketRate6";
      public static final String DROP_RATE6_VALUE = "Net_PacketDropRate6";
      public static final String THROUGHPUT_VALUE = "Net_Throughput";
    }
  }

  public enum ThreadPoolDimension implements MetricDimension, JooqFieldValue {
    THREAD_POOL_TYPE(Constants.TYPE_VALUE);

    private final String value;

    ThreadPoolDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    @Override
    public Field<String> getField() {
      return DSL.field(DSL.name(this.value), String.class);
    }

    @Override
    public String getName() {
      return value;
    }

    public static class Constants {
      public static final String TYPE_VALUE = "ThreadPoolType";
    }
  }

  public enum ThreadPoolValue implements MetricValue {
    THREADPOOL_QUEUE_SIZE(Constants.QUEUE_SIZE_VALUE),
    THREADPOOL_REJECTED_REQS(Constants.REJECTED_VALUE),
    THREADPOOL_TOTAL_THREADS(Constants.THREADS_COUNT_VALUE),
    THREADPOOL_ACTIVE_THREADS(Constants.THREADS_ACTIVE_VALUE),
    THREADPOOL_QUEUE_LATENCY(Constants.QUEUE_LATENCY_VALUE),
    THREADPOOL_QUEUE_CAPACITY(Constants.QUEUE_CAPACITY_VALUE);

    private final String value;

    ThreadPoolValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String QUEUE_SIZE_VALUE = "ThreadPool_QueueSize";
      public static final String REJECTED_VALUE = "ThreadPool_RejectedReqs";
      public static final String THREADS_COUNT_VALUE = "ThreadPool_TotalThreads";
      public static final String THREADS_ACTIVE_VALUE = "ThreadPool_ActiveThreads";
      public static final String QUEUE_LATENCY_VALUE = "ThreadPool_QueueLatency";
      public static final String QUEUE_CAPACITY_VALUE = "ThreadPool_QueueCapacity";
    }
  }

  //list of threadpools
  public enum ThreadPoolType {
    ANALYZE(Constants.ANALYZE_NAME),
    FETCH_SHARD_STARTED(Constants.FETCH_SHARD_STARTED_NAME),
    FETCH_SHARD_STORE(Constants.FETCH_SHARD_STORE_NAME),
    FLUSH(Constants.FLUSH_NAME),
    FORCE_MERGE(Constants.FORCE_MERGE_NAME),
    GENERIC(Constants.GENERIC_NAME),
    GET(Constants.GET_NAME),
    LISTENER(Constants.LISTENER_NAME),
    MANAGEMENT(Constants.MANAGEMENT_NAME),
    OPEN_DISTRO_JOB_SCHEDULER(Constants.OPEN_DISTRO_JOB_SCHEDULER_NAME),
    REFRESH(Constants.REFRESH_NAME),
    SEARCH(Constants.SEARCH_NAME),
    SEARCH_THROTTLED(Constants.SEARCH_THROTTLED_NAME),
    SNAPSHOT(Constants.SNAPSHOT_NAME),
    SNAPSHOT_SEGMENTS(Constants.SNAPSHOT_SEGMENTS_NAME),
    SQL_WORKER(Constants.SQL_WORKER_NAME),
    WARMER(Constants.WARMER_NAME),
    WRITE(Constants.WRITE_NAME);

    private final String value;

    ThreadPoolType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String ANALYZE_NAME = "analyze";
      public static final String FETCH_SHARD_STARTED_NAME = "fetch_shard_started";
      public static final String FETCH_SHARD_STORE_NAME = "fetch_shard_store";
      public static final String FLUSH_NAME = "flush";
      public static final String FORCE_MERGE_NAME = "force_merge";
      public static final String GENERIC_NAME = "generic";
      public static final String GET_NAME = "get";
      public static final String LISTENER_NAME = "listener";
      public static final String MANAGEMENT_NAME = "management";
      public static final String OPEN_DISTRO_JOB_SCHEDULER_NAME = "open_distro_job_scheduler";
      public static final String REFRESH_NAME = "refresh";
      public static final String SEARCH_NAME = "search";
      public static final String SEARCH_THROTTLED_NAME = "search_throttled";
      public static final String SNAPSHOT_NAME = "snapshot";
      public static final String SNAPSHOT_SEGMENTS_NAME = "snapshot_segments";
      public static final String SQL_WORKER_NAME = "sql-worker";
      public static final String WARMER_NAME = "warmer";
      public static final String WRITE_NAME = "write";
    }
  }

  // extra dimension values come from other places (e.g., file path) instead
  // of metric files themselves
  public enum ShardStatsDerivedDimension implements MetricDimension {
    INDEX_NAME(Constants.INDEX_NAME_VALUE),
    SHARD_ID(Constants.SHARD_ID_VALUE);

    private final String value;

    ShardStatsDerivedDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String INDEX_NAME_VALUE = CommonDimension.INDEX_NAME.toString();

      public static final String SHARD_ID_VALUE = CommonDimension.SHARD_ID.toString();
    }
  }

  public enum ShardStatsValue implements MetricValue {
    INDEXING_THROTTLE_TIME(Constants.INDEXING_THROTTLE_TIME_VALUE),
    CACHE_QUERY_HIT(Constants.QUEY_CACHE_HIT_COUNT_VALUE),
    CACHE_QUERY_MISS(Constants.QUERY_CACHE_MISS_COUNT_VALUE),
    CACHE_QUERY_SIZE(Constants.QUERY_CACHE_IN_BYTES_VALUE),
    CACHE_FIELDDATA_EVICTION(Constants.FIELDDATA_EVICTION_VALUE),
    CACHE_FIELDDATA_SIZE(Constants.FIELD_DATA_IN_BYTES_VALUE),
    CACHE_REQUEST_HIT(Constants.REQUEST_CACHE_HIT_COUNT_VALUE),
    CACHE_REQUEST_MISS(Constants.REQUEST_CACHE_MISS_COUNT_VALUE),
    CACHE_REQUEST_EVICTION(Constants.REQUEST_CACHE_EVICTION_VALUE),
    CACHE_REQUEST_SIZE(Constants.REQUEST_CACHE_IN_BYTES_VALUE),
    REFRESH_EVENT(Constants.REFRESH_COUNT_VALUE),
    REFRESH_TIME(Constants.REFRESH_TIME_VALUE),
    FLUSH_EVENT(Constants.FLUSH_COUNT_VALUE),
    FLUSH_TIME(Constants.FLUSH_TIME_VALUE),
    MERGE_EVENT(Constants.MERGE_COUNT_VALUE),
    MERGE_TIME(Constants.MERGE_TIME_VALUE),
    MERGE_CURRENT_EVENT(Constants.MERGE_CURRENT_VALUE),
    INDEXING_BUFFER(Constants.INDEX_BUFFER_BYTES_VALUE),
    SEGMENTS_TOTAL(Constants.SEGMENTS_COUNT_VALUE),
    SEGMENTS_MEMORY(Constants.SEGMENTS_MEMORY_VALUE),
    TERMS_MEMORY(Constants.TERMS_MEMORY_VALUE),
    STORED_FIELDS_MEMORY(Constants.STORED_FIELDS_MEMORY_VALUE),
    TERM_VECTOR_MEMORY(Constants.TERM_VECTOR_MEMORY_VALUE),
    NORMS_MEMORY(Constants.NORMS_MEMORY_VALUE),
    POINTS_MEMORY(Constants.POINTS_MEMORY_VALUE),
    DOC_VALUES_MEMORY(Constants.DOC_VALUES_MEMORY_VALUE),
    INDEX_WRITER_MEMORY(Constants.INDEX_WRITER_MEMORY_VALUE),
    VERSION_MAP_MEMORY(Constants.VERSION_MAP_MEMORY_VALUE),
    BITSET_MEMORY(Constants.BITSET_MEMORY_VALUE),
    SHARD_SIZE_IN_BYTES(Constants.SHARD_SIZE_IN_BYTES_VALUE);

    private final String value;

    ShardStatsValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String INDEXING_THROTTLE_TIME_VALUE = "Indexing_ThrottleTime";

      public static final String QUEY_CACHE_HIT_COUNT_VALUE = "Cache_Query_Hit";

      public static final String QUERY_CACHE_MISS_COUNT_VALUE = "Cache_Query_Miss";

      public static final String QUERY_CACHE_IN_BYTES_VALUE = "Cache_Query_Size";

      public static final String FIELDDATA_EVICTION_VALUE = "Cache_FieldData_Eviction";

      public static final String FIELD_DATA_IN_BYTES_VALUE = "Cache_FieldData_Size";

      public static final String REQUEST_CACHE_HIT_COUNT_VALUE = "Cache_Request_Hit";

      public static final String REQUEST_CACHE_MISS_COUNT_VALUE = "Cache_Request_Miss";

      public static final String REQUEST_CACHE_EVICTION_VALUE = "Cache_Request_Eviction";

      public static final String REQUEST_CACHE_IN_BYTES_VALUE = "Cache_Request_Size";

      public static final String REFRESH_COUNT_VALUE = "Refresh_Event";

      public static final String REFRESH_TIME_VALUE = "Refresh_Time";

      public static final String FLUSH_COUNT_VALUE = "Flush_Event";

      public static final String FLUSH_TIME_VALUE = "Flush_Time";

      public static final String MERGE_COUNT_VALUE = "Merge_Event";

      public static final String MERGE_TIME_VALUE = "Merge_Time";

      public static final String MERGE_CURRENT_VALUE = "Merge_CurrentEvent";

      public static final String INDEX_BUFFER_BYTES_VALUE = "Indexing_Buffer";

      public static final String SEGMENTS_COUNT_VALUE = "Segments_Total";

      public static final String SEGMENTS_MEMORY_VALUE = "Segments_Memory";

      public static final String TERMS_MEMORY_VALUE = "Terms_Memory";

      public static final String STORED_FIELDS_MEMORY_VALUE = "StoredFields_Memory";

      public static final String TERM_VECTOR_MEMORY_VALUE = "TermVectors_Memory";

      public static final String NORMS_MEMORY_VALUE = "Norms_Memory";

      public static final String POINTS_MEMORY_VALUE = "Points_Memory";

      public static final String DOC_VALUES_MEMORY_VALUE = "DocValues_Memory";

      public static final String INDEX_WRITER_MEMORY_VALUE = "IndexWriter_Memory";

      public static final String VERSION_MAP_MEMORY_VALUE = "VersionMap_Memory";

      public static final String BITSET_MEMORY_VALUE = "Bitset_Memory";

      public static final String SHARD_SIZE_IN_BYTES_VALUE = "Shard_Size_In_Bytes";
    }
  }

  public enum MasterPendingValue implements MetricValue {
    MASTER_PENDING_QUEUE_SIZE(Constants.PENDING_TASKS_COUNT_VALUE);

    private final String value;

    MasterPendingValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String PENDING_TASKS_COUNT_VALUE = "Master_PendingQueueSize";
    }
  }

  public enum MasterThrottlingValue implements MetricValue {
    /**
     * Sum of total pending tasks throttled by master node.
     */
    MASTER_THROTTLED_PENDING_TASK_COUNT(MasterThrottlingValue.Constants.THROTTLED_PENDING_TASK_COUNT),
    /**
     * Number of pending tasks on which data nodes are actively performing retries.
     */
    DATA_RETRYING_TASK_COUNT(MasterThrottlingValue.Constants.RETRYING_TASK_COUNT);

    private final String value;

    MasterThrottlingValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String THROTTLED_PENDING_TASK_COUNT = "Master_ThrottledPendingTasksCount";
      public static final String RETRYING_TASK_COUNT = "Data_RetryingPendingTasksCount";
    }
  }

  public enum OSMetrics {
    CPU_UTILIZATION(Constants.CPU_VALUE),
    PAGING_MAJ_FLT_RATE(Constants.PAGING_MAJFLT_VALUE),
    PAGING_MIN_FLT_RATE(Constants.PAGING_MINFLT_VALUE),
    PAGING_RSS(Constants.RSS_VALUE),
    SCHED_RUNTIME(Constants.RUNTIME_VALUE),
    SCHED_WAITTIME(Constants.WAITTIME_VALUE),
    SCHED_CTX_RATE(Constants.CTXRATE_VALUE),
    HEAP_ALLOC_RATE(Constants.HEAP_ALLOC_VALUE),
    IO_READ_THROUGHPUT(Constants.READ_THROUGHPUT_VALUE),
    IO_WRITE_THROUGHPUT(Constants.WRITE_THROUGHPUT_VALUE),
    IO_TOT_THROUGHPUT(Constants.TOTAL_THROUGHPUT_VALUE),
    IO_READ_SYSCALL_RATE(Constants.READ_SYSCALL_RATE_VALUE),
    IO_WRITE_SYSCALL_RATE(Constants.WRITE_SYSCALL_RATE_VALUE),
    IO_TOTAL_SYSCALL_RATE(Constants.TOTAL_SYSCALL_RATE_VALUE),
    THREAD_BLOCKED_TIME(Constants.BLOCKED_TIME_VALUE),
    THREAD_BLOCKED_EVENT(Constants.BLOCKED_COUNT_VALUE);

    private final String value;

    OSMetrics(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String CPU_VALUE = "CPU_Utilization";
      public static final String PAGING_MAJFLT_VALUE = "Paging_MajfltRate";
      public static final String PAGING_MINFLT_VALUE = "Paging_MinfltRate";
      public static final String RSS_VALUE = "Paging_RSS";
      public static final String RUNTIME_VALUE = "Sched_Runtime";
      public static final String WAITTIME_VALUE = "Sched_Waittime";
      public static final String CTXRATE_VALUE = "Sched_CtxRate";
      public static final String HEAP_ALLOC_VALUE = "Heap_AllocRate";
      public static final String READ_THROUGHPUT_VALUE = "IO_ReadThroughput";
      public static final String WRITE_THROUGHPUT_VALUE = "IO_WriteThroughput";
      public static final String TOTAL_THROUGHPUT_VALUE = "IO_TotThroughput";
      public static final String READ_SYSCALL_RATE_VALUE = "IO_ReadSyscallRate";
      public static final String WRITE_SYSCALL_RATE_VALUE = "IO_WriteSyscallRate";
      public static final String TOTAL_SYSCALL_RATE_VALUE = "IO_TotalSyscallRate";
      public static final String BLOCKED_TIME_VALUE = "Thread_Blocked_Time";
      public static final String BLOCKED_COUNT_VALUE = "Thread_Blocked_Event";
    }
  }

  public enum MasterMetricDimensions implements MetricDimension {
    MASTER_TASK_PRIORITY("MasterTaskPriority"),
    MASTER_TASK_TYPE("MasterTaskType"),
    MASTER_TASK_METADATA("MasterTaskMetadata"),
    MASTER_TASK_QUEUE_TIME("MasterTaskQueueTime"),
    MASTER_TASK_RUN_TIME("MasterTaskRunTime"),
    MASTER_TASK_INSERT_ORDER("MasterTaskInsertOrder");

    private final String value;

    MasterMetricDimensions(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public enum MasterMetricValues implements MetricValue {
    // -todo : Migrate to CommonMetric.Constants
    MASTER_TASK_QUEUE_TIME("Master_Task_Queue_Time"),
    MASTER_TASK_RUN_TIME("Master_Task_Run_Time"),
    START_TIME("StartTime"),
    FINISH_TIME("FinishTime");

    private final String value;

    MasterMetricValues(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public enum HttpDimension implements MetricDimension {
    EXCEPTION(Constants.EXCEPTION_VALUE),
    HTTP_RESP_CODE(Constants.HTTP_RESP_CODE_VALUE),
    INDICES(Constants.INDICES_VALUE);

    private final String value;

    HttpDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String INDICES_VALUE = "Indices";
      public static final String EXCEPTION_VALUE = CommonDimension.EXCEPTION.toString();
      public static final String HTTP_RESP_CODE_VALUE = "HTTPRespCode";
    }
  }

  public enum HttpMetric implements MetricValue {
    START_TIME(Constants.START_TIME_VALUE),
    HTTP_REQUEST_DOCS(Constants.HTTP_REQUEST_DOCS_VALUE),
    FINISH_TIME(Constants.FINISH_TIME_VALUE),
    HTTP_TOTAL_REQUESTS(Constants.HTTP_TOTAL_REQUESTS_VALUE);

    private final String value;

    HttpMetric(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String START_TIME_VALUE = CommonMetric.START_TIME.toString();
      public static final String FINISH_TIME_VALUE = CommonMetric.FINISH_TIME.toString();
      public static final String HTTP_REQUEST_DOCS_VALUE = "HTTP_RequestDocs";
      public static final String HTTP_TOTAL_REQUESTS_VALUE = "HTTP_TotalRequests";
    }
  }

  public enum ShardBulkDimension implements MetricDimension {
    INDEX_NAME(Constants.INDEXNAME_VALUE),
    SHARD_ID(Constants.SHARDID_VALUE),
    PRIMARY(Constants.PRIMARY_VALUE),
    EXCEPTION(Constants.EXCEPTION_VALUE),
    FAILED(Constants.FAILED_VALUE);

    private final String value;

    ShardBulkDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String INDEXNAME_VALUE = CommonDimension.INDEX_NAME.toString();
      public static final String SHARDID_VALUE = CommonDimension.SHARD_ID.toString();
      public static final String PRIMARY_VALUE = "Primary";
      public static final String EXCEPTION_VALUE = CommonDimension.EXCEPTION.toString();
      public static final String FAILED_VALUE = CommonDimension.FAILED.toString();
    }
  }

  public enum ShardBulkMetric implements MetricValue {
    START_TIME(Constants.START_TIME_VALUE),
    ITEM_COUNT(Constants.ITEM_COUNT_VALUE),
    FINISH_TIME(Constants.FINISH_TIME_VALUE),
    LATENCY(Constants.LATENCY_VALUE),
    DOC_COUNT(Constants.DOC_COUNT);

    private final String value;

    ShardBulkMetric(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String START_TIME_VALUE = CommonMetric.START_TIME.toString();
      public static final String ITEM_COUNT_VALUE = "ItemCount";
      public static final String FINISH_TIME_VALUE = CommonMetric.FINISH_TIME.toString();
      public static final String LATENCY_VALUE = CommonMetric.LATENCY.toString();
      public static final String DOC_COUNT = "ShardBulkDocs";
    }
  }

  public enum ShardOperationMetric implements MetricValue {
    SHARD_OP_COUNT(Constants.SHARD_OP_COUNT_VALUE);

    private final String value;

    ShardOperationMetric(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String SHARD_OP_COUNT_VALUE = "ShardEvents";
    }
  }
  /*
   * column names of FollowerCheck_Latency table
   * SourceNodeId | TargetNodeID | sum | avg | min |max
   *
   * column names of LeaderCheck_Latency table
   * SourceNodeId | TargetNodeID | sum | avg | min |max
   *
   * column names of FollowerCheck_Failure table
   * SourceNodeId | TargetNodeID | sum | avg | min |max
   *
   * column names of LeaderCheck_Failure table
   * SourceNodeId | TargetNodeID | sum | avg | min |max
   *
   * <p>Example:
   * chMe07whRwGrOAqyLTP9vw|hgi7an4RwGrOAqyLTP9vw|1.0|0.2|0.0|1.0
   */

  public enum FaultDetectionMetric implements MetricValue {
    FOLLOWER_CHECK_LATENCY(Constants.FOLLOWER_CHECK_LATENCY),
    LEADER_CHECK_LATENCY(Constants.LEADER_CHECK_LATENCY),
    FOLLOWER_CHECK_FAILURE(Constants.FOLLOWER_CHECK_FAILURE),
    LEADER_CHECK_FAILURE(Constants.LEADER_CHECK_FAILURE);

    private final String value;

    FaultDetectionMetric(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String FOLLOWER_CHECK_LATENCY = "FollowerCheck_Latency";
      public static final String LEADER_CHECK_LATENCY = "LeaderCheck_Latency";
      public static final String FOLLOWER_CHECK_FAILURE = "FollowerCheck_Failure";
      public static final String LEADER_CHECK_FAILURE = "LeaderCheck_Failure";
    }
  }

  public enum FaultDetectionDimension implements MetricDimension {
    SOURCE_NODE_ID(Constants.SOURCE_NODE_ID),
    TARGET_NODE_ID(Constants.TARGET_NODE_ID);

    private final String value;

    FaultDetectionDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String SOURCE_NODE_ID = "SourceNodeID";
      public static final String TARGET_NODE_ID = "TargetNodeID";
    }
  }

  public enum CommonDimension implements MetricDimension {
    INDEX_NAME(Constants.INDEX_NAME_VALUE),
    OPERATION(Constants.OPERATION_VALUE),
    SHARD_ROLE(Constants.SHARD_ROLE_VALUE),
    SHARD_ID(Constants.SHARDID_VALUE),
    EXCEPTION(Constants.EXCEPTION_VALUE),
    FAILED(Constants.FAILED_VALUE);

    private final String value;

    CommonDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String INDEX_NAME_VALUE = "IndexName";
      public static final String SHARDID_VALUE = "ShardID";
      public static final String OPERATION_VALUE = "Operation";
      public static final String SHARD_ROLE_VALUE = "ShardRole";
      public static final String EXCEPTION_VALUE = "Exception";
      public static final String FAILED_VALUE = "Failed";
    }
  }

  public enum CommonMetric {
    START_TIME(Constants.START_TIME_VALUE),
    FINISH_TIME(Constants.FINISH_TIME_VALUE),
    LATENCY(Constants.LATENCY_VALUE);

    private final String value;

    CommonMetric(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String START_TIME_VALUE = "StartTime";
      public static final String FINISH_TIME_VALUE = "FinishTime";
      public static final String LATENCY_VALUE = "Latency";
    }
  }

  public enum EmptyDimension implements MetricDimension {
    EMPTY("");

    private final String value;

    EmptyDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public enum AggregatedOSDimension implements MetricDimension {
    INDEX_NAME(CommonDimension.INDEX_NAME.toString()),
    OPERATION(CommonDimension.OPERATION.toString()),
    SHARD_ROLE(CommonDimension.SHARD_ROLE.toString()),
    SHARD_ID(CommonDimension.SHARD_ID.toString());

    private final String value;

    AggregatedOSDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public enum LatencyDimension implements MetricDimension {
    OPERATION(CommonDimension.OPERATION.toString()),
    EXCEPTION(CommonDimension.EXCEPTION.toString()),
    INDICES(HttpDimension.INDICES.toString()),
    HTTP_RESP_CODE(HttpDimension.HTTP_RESP_CODE.toString()),
    SHARD_ID(CommonDimension.SHARD_ID.toString()),
    INDEX_NAME(CommonDimension.INDEX_NAME.toString()),
    SHARD_ROLE(CommonDimension.SHARD_ROLE.toString());

    private final String value;

    LatencyDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public enum HttpOnlyDimension implements MetricDimension {
    OPERATION(CommonDimension.OPERATION.toString()),
    EXCEPTION(CommonDimension.EXCEPTION.toString()),
    INDICES(HttpDimension.INDICES.toString()),
    HTTP_RESP_CODE(HttpDimension.HTTP_RESP_CODE.toString());

    private final String value;

    HttpOnlyDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  /*
   * column names of Shard_State table
   * IndexName | ShardID | ShardType | NodeName | Shard_State | sum | avg | min |max
   *
   * <p>Example:
   * pmc|4|p|elasticsearch1|UNASSIGNED|1.0|1.0|1.0|1.0
   * pmc|2|p|elasticsearch2|INITIALIZING|1.0|1.0|1.0|1.0
   */
  public enum ShardStateDimension implements MetricDimension {
    INDEX_NAME(CommonDimension.INDEX_NAME.toString()),
    SHARD_ID(CommonDimension.SHARD_ID.toString()),
    SHARD_TYPE(Constants.SHARD_TYPE),
    NODE_NAME(Constants.NODE_NAME),
    SHARD_STATE(Constants.SHARD_STATE);

    private final String value;

    ShardStateDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String NODE_NAME = "NodeName";
      public static final String SHARD_TYPE = "ShardType";
      public static final String SHARD_STATE = "Shard_State";
    }
  }

  public enum ShardType {
    SHARD_PRIMARY(Constants.SHARD_PRIMARY),
    SHARD_REPLICA(Constants.SHARD_REPLICA);

    private final String value;

    ShardType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String SHARD_PRIMARY = "p";
      public static final String SHARD_REPLICA = "r";
    }
  }

  public enum ShardStateValue implements MetricValue {
    SHARD_STATE(Constants.SHARD_STATE);

    private final String value;

    ShardStateValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String SHARD_STATE = "Shard_State";
    }
  }

  /** Enumeration of the indexing stages. */
  public enum IndexingStage {
    COORDINATING(Constants.COORDINATING_VALUE),
    PRIMARY(Constants.PRIMARY_VALUE),
    REPLICA(Constants.REPLICA_VALUE);

    private final String value;

    IndexingStage(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String COORDINATING_VALUE = "Coordinating";
      public static final String PRIMARY_VALUE = "Primary";
      public static final String REPLICA_VALUE = "Replica";
    }
  }

  /** Enumeration of the Shard Indexing Pressure Dimension*/
  public enum ShardIndexingPressureDimension implements  MetricDimension {
    INDEXING_STAGE(Constants.INDEXING_STAGE),
    INDEX_NAME(Constants.INDEX_NAME_VALUE),
    SHARD_ID(Constants.SHARD_ID_VALUE);

    private final String value;

    ShardIndexingPressureDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String INDEXING_STAGE = "IndexingStage";
      public static final String INDEX_NAME_VALUE = "IndexName";
      public static final String SHARD_ID_VALUE = "ShardID";
    }
  }

  /**
   * Column names of Rejection_Count table
   * NodeRole | IndexName | ShardID | sum | avg | min |max
   * Column names of Current_Bytes table
   * NodeRole | IndexName | ShardID | sum | avg | min |max
   * Column names of Current_Limits table
   * NodeRole | IndexName | ShardID | sum | avg | min |max
   * Column names of Average_Window_Throughput table
   * NodeRole | IndexName | ShardID | sum | avg | min |max
   * Column names of Last_Successful_Timestamp table
   * NodeRole | IndexName | ShardID | sum | avg | min |max
   *
   * <p>Example:
   * Coordinating|pmc|4|1.0|1.0|1.0|1.0
   * Primary|pmc|2|1.0|1.0|1.0|1.0
   */
  public enum ShardIndexingPressureValue implements MetricValue {
    REJECTION_COUNT(Constants.REJECTION_COUNT_VALUE),
    CURRENT_BYTES(Constants.CURRENT_BYTES),
    CURRENT_LIMITS(Constants.CURRENT_LIMITS),
    AVERAGE_WINDOW_THROUGHPUT(Constants.AVERAGE_WINDOW_THROUGHPUT),
    LAST_SUCCESSFUL_TIMESTAMP(Constants.LAST_SUCCESSFUL_TIMESTAMP);

    private final String value;

    ShardIndexingPressureValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String REJECTION_COUNT_VALUE = "Indexing_Pressure_Rejection_Count";
      public static final String CURRENT_BYTES = "Indexing_Pressure_Current_Bytes";
      public static final String CURRENT_LIMITS = "Indexing_Pressure_Current_Limits";
      public static final String AVERAGE_WINDOW_THROUGHPUT = "Indexing_Pressure_Average_Window_Throughput";
      public static final String LAST_SUCCESSFUL_TIMESTAMP = "Indexing_Pressure_Last_Successful_Timestamp";
    }
  }

  /*
   * Column names of AdmissionControl_RejectionCount table
   * ControllerName | sum | avg | min | max
   *
   * Column names of AdmissionControl_ThresholdValue table
   * ControllerName | sum | avg | min | max
   *
   * Column names of AdmissionControl_CurrentValue table
   * ControllerName | sum | avg | min | max
   *
   * Example:
   * Global_JVMMP|41.0|8.2|8.0|9.0
   * Request_Size|0.0|0.0|0.0|0.0
   */
  public enum AdmissionControlDimension implements MetricDimension, JooqFieldValue {
    CONTROLLER_NAME(Constants.CONTROLLER_NAME);

    private final String value;

    AdmissionControlDimension(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    @Override
    public Field<String> getField() {
      return DSL.field(DSL.name(this.value), String.class);
    }

    @Override
    public String getName() {
      return value;
    }

    public static class Constants {
      public static final String CONTROLLER_NAME = "ControllerName";
    }
  }

  public enum AdmissionControlValue implements MetricValue {
    CURRENT_VALUE(Constants.CURRENT_VALUE),
    THRESHOLD_VALUE(Constants.THRESHOLD_VALUE),
    REJECTION_COUNT(Constants.REJECTION_COUNT);

    private final String value;

    AdmissionControlValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String CURRENT_VALUE = "AdmissionControl_CurrentValue";
      public static final String THRESHOLD_VALUE = "AdmissionControl_ThresholdValue";
      public static final String REJECTION_COUNT = "AdmissionControl_RejectionCount";
    }
  }

  public enum MetricUnits {
    CORES(Constants.CORES_VALUE),
    COUNT_PER_SEC(Constants.COUNT_PER_SEC_VALUE),
    COUNT(Constants.COUNT_VALUE),
    PAGES(Constants.PAGES_VALUE),
    SEC_PER_CONTEXT_SWITCH(Constants.SEC_PER_CONTEXT_SWITCH_VALUE),
    BYTE_PER_SEC(Constants.BYTE_PER_SEC_VALUE),
    SEC_PER_EVENT(Constants.SEC_PER_EVENT_VALUE),
    MILLISECOND(Constants.MILLISECOND_VALUE),
    BYTE(Constants.BYTE_VALUE),
    PERCENT(Constants.PERCENT_VALUE),
    MEGABYTE_PER_SEC(Constants.MEGABYTE_PER_SEC_VALUE),
    SEGMENT_PER_FLOW(Constants.SEGMENT_PER_FLOW_VALUE),
    BYTE_PER_FLOW(Constants.BYTE_PER_FLOW_VALUE),
    PACKET_PER_SEC(Constants.PACKET_PER_SEC_VALUE);

    private final String value;

    MetricUnits(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String CORES_VALUE = "cores";
      public static final String COUNT_PER_SEC_VALUE = "count/s";
      public static final String COUNT_VALUE = "count";
      public static final String PAGES_VALUE = "pages";
      public static final String SEC_PER_CONTEXT_SWITCH_VALUE = "s/ctxswitch";
      public static final String BYTE_PER_SEC_VALUE = "B/s";
      public static final String SEC_PER_EVENT_VALUE = "s/event";
      public static final String MILLISECOND_VALUE = "ms";
      public static final String BYTE_VALUE = "B";
      public static final String PERCENT_VALUE = "%";
      public static final String MEGABYTE_PER_SEC_VALUE = "MB/s";
      public static final String SEGMENT_PER_FLOW_VALUE = "segments/flow";
      public static final String BYTE_PER_FLOW_VALUE = "B/flow";
      public static final String PACKET_PER_SEC_VALUE = "packets/s";
    }
  }
}
