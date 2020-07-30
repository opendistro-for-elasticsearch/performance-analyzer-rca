// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: inter_node_rpc_service.proto

package com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc;

/**
 * Protobuf enum {@code com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricEnum}
 */
public enum MetricEnum
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <pre>
   * JVM
   * </pre>
   *
   * <code>HEAP_USAGE = 0 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  HEAP_USAGE(0),
  /**
   * <code>PROMOTION_RATE = 1 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  PROMOTION_RATE(1),
  /**
   * <code>MINOR_GC = 2 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  MINOR_GC(2),
  /**
   * <pre>
   * hardware
   * </pre>
   *
   * <code>CPU_USAGE = 3 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  CPU_USAGE(3),
  /**
   * <code>TOTAL_THROUGHPUT = 4 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  TOTAL_THROUGHPUT(4),
  /**
   * <code>TOTAL_SYS_CALLRATE = 5 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  TOTAL_SYS_CALLRATE(5),
  /**
   * <pre>
   * threadpool
   * </pre>
   *
   * <code>QUEUE_REJECTION = 6 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  QUEUE_REJECTION(6),
  /**
   * <code>QUEUE_CAPACITY = 7 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  QUEUE_CAPACITY(7),
  /**
   * <pre>
   * cache
   * </pre>
   *
   * <code>CACHE_EVICTION = 10 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  CACHE_EVICTION(10),
  /**
   * <code>CACHE_HIT = 11 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  CACHE_HIT(11),
  /**
   * <code>CACHE_MAX_SIZE = 12 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  CACHE_MAX_SIZE(12),
  UNRECOGNIZED(-1),
  ;

  /**
   * <pre>
   * JVM
   * </pre>
   *
   * <code>HEAP_USAGE = 0 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  public static final int HEAP_USAGE_VALUE = 0;
  /**
   * <code>PROMOTION_RATE = 1 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  public static final int PROMOTION_RATE_VALUE = 1;
  /**
   * <code>MINOR_GC = 2 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  public static final int MINOR_GC_VALUE = 2;
  /**
   * <pre>
   * hardware
   * </pre>
   *
   * <code>CPU_USAGE = 3 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  public static final int CPU_USAGE_VALUE = 3;
  /**
   * <code>TOTAL_THROUGHPUT = 4 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  public static final int TOTAL_THROUGHPUT_VALUE = 4;
  /**
   * <code>TOTAL_SYS_CALLRATE = 5 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  public static final int TOTAL_SYS_CALLRATE_VALUE = 5;
  /**
   * <pre>
   * threadpool
   * </pre>
   *
   * <code>QUEUE_REJECTION = 6 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  public static final int QUEUE_REJECTION_VALUE = 6;
  /**
   * <code>QUEUE_CAPACITY = 7 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  public static final int QUEUE_CAPACITY_VALUE = 7;
  /**
   * <pre>
   * cache
   * </pre>
   *
   * <code>CACHE_EVICTION = 10 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  public static final int CACHE_EVICTION_VALUE = 10;
  /**
   * <code>CACHE_HIT = 11 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  public static final int CACHE_HIT_VALUE = 11;
  /**
   * <code>CACHE_MAX_SIZE = 12 [(.com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.additional_fields) = { ... }</code>
   */
  public static final int CACHE_MAX_SIZE_VALUE = 12;


  public final int getNumber() {
    if (this == UNRECOGNIZED) {
      throw new java.lang.IllegalArgumentException(
          "Can't get the number of an unknown enum value.");
    }
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static MetricEnum valueOf(int value) {
    return forNumber(value);
  }

  public static MetricEnum forNumber(int value) {
    switch (value) {
      case 0: return HEAP_USAGE;
      case 1: return PROMOTION_RATE;
      case 2: return MINOR_GC;
      case 3: return CPU_USAGE;
      case 4: return TOTAL_THROUGHPUT;
      case 5: return TOTAL_SYS_CALLRATE;
      case 6: return QUEUE_REJECTION;
      case 7: return QUEUE_CAPACITY;
      case 10: return CACHE_EVICTION;
      case 11: return CACHE_HIT;
      case 12: return CACHE_MAX_SIZE;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<MetricEnum>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      MetricEnum> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<MetricEnum>() {
          public MetricEnum findValueByNumber(int number) {
            return MetricEnum.forNumber(number);
          }
        };

  public final com.google.protobuf.Descriptors.EnumValueDescriptor
      getValueDescriptor() {
    return getDescriptor().getValues().get(ordinal());
  }
  public final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptorForType() {
    return getDescriptor();
  }
  public static final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptor() {
    return com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PANetworking.getDescriptor().getEnumTypes().get(1);
  }

  private static final MetricEnum[] VALUES = values();

  public static MetricEnum valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    if (desc.getIndex() == -1) {
      return UNRECOGNIZED;
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private MetricEnum(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricEnum)
}

