/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.AdditionalFields;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PANetworking;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;

/**
 * A utility class to parse and build grpc ResourceType
 */
public class ResourceUtil {

  // JVM resource
  public static Resource OLD_GEN_HEAP_USAGE = Resource.newBuilder()
      .setResource(ResourceEnum.OLD_GEN)
      .setMetric(MetricEnum.HEAP_USAGE).build();
  public static Resource YOUNG_GEN_PROMOTION_RATE = Resource.newBuilder()
      .setResource(ResourceEnum.YOUNG_GEN)
      .setMetric(MetricEnum.PROMOTION_RATE).build();

  // hardware resource
  public static Resource CPU_USAGE = Resource.newBuilder()
      .setResource(ResourceEnum.CPU)
      .setMetric(MetricEnum.CPU_USAGE).build();
  public static Resource IO_TOTAL_THROUGHPUT = Resource.newBuilder()
      .setResource(ResourceEnum.IO)
      .setMetric(MetricEnum.TOTAL_THROUGHPUT).build();
  public static Resource IO_TOTAL_SYS_CALLRATE = Resource.newBuilder()
      .setResource(ResourceEnum.IO)
      .setMetric(MetricEnum.TOTAL_SYS_CALLRATE).build();

  // thread pool
  public static Resource WRITE_QUEUE_REJECTION = Resource.newBuilder()
      .setResource(ResourceEnum.WRITE_THREADPOOL)
      .setMetric(MetricEnum.QUEUE_REJECTION).build();
  public static Resource SEARCH_QUEUE_REJECTION = Resource.newBuilder()
      .setResource(ResourceEnum.SEARCH_THREADPOOL)
      .setMetric(MetricEnum.QUEUE_REJECTION).build();

  /**
   * Read the resourceType name from the ResourceType object
   * @param resource grpc Resource object
   * @return resource type name
   */
  public static String getResourceTypeName(Resource resource) {
    return resource.getResource().getValueDescriptor().getOptions()
          .getExtension(PANetworking.additionalFields).getName();
  }

  /**
   * Read the resourceType unit type from the ResourceType object
   * @param resource grpc ResourceType object
   * @return resource unit type
   */
  public static String getResourceTypeUnit(Resource resource) {
    AdditionalFields resourceMetricOptions = resource.getMetric().getValueDescriptor().getOptions()
        .getExtension(PANetworking.additionalFields);
    return resourceMetricOptions.getName() + "(" + resourceMetricOptions.getDescription() + ")";
  }

  /**
   * Map resourceTypeName to its enum object
   * @param resourceEnumValue resource enum value
   * @param metricEnumValue metric enum value
   * @return ResourceType enum object
   */
  public static Resource buildResourceType(int resourceEnumValue, int metricEnumValue) {
    Resource.Builder builder = Resource.newBuilder();
    builder.setResourceValue(resourceEnumValue);
    builder.setMetricValue(metricEnumValue);
    return builder.build();
  }
}
