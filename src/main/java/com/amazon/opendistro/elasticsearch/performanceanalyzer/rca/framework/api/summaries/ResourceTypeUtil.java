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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.HardwareEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.JvmEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PANetworking;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType.ResourceTypeOneofCase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceTypeOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ProtocolMessageEnum;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A utility class to parse and build grpc ResourceType
 */
public class ResourceTypeUtil {
  private static final Logger LOG = LogManager.getLogger(ResourceTypeUtil.class);

  static final String UNKNOWN_RESOURCE_TYPE_NAME = "unknown resource type";
  static final String UNKNOWN_RESOURCE_TYPE_UNIT = "unknown resource unit type";

  /**
   * Read the resourceType name from the ResourceType object
   * @param resourceType grpc ResourceType object
   * @return resource type name
   */
  public static String getResourceTypeName(ResourceType resourceType) {
    String resourceName = UNKNOWN_RESOURCE_TYPE_NAME;
    ResourceTypeOptions resourceTypeOptions = ResourceTypeUtil.getResourceTypeOptions(resourceType);
    if (resourceTypeOptions != null) {
      resourceName = resourceTypeOptions.getResourceTypeName();
    }
    return resourceName;
  }

  /**
   * Read the resourceType unit type from the ResourceType object
   * @param resourceType grpc ResourceType object
   * @return resource unit type
   */
  public static String getResourceTypeUnit(ResourceType resourceType) {
    String resourceName = UNKNOWN_RESOURCE_TYPE_UNIT;
    ResourceTypeOptions resourceTypeOptions = ResourceTypeUtil.getResourceTypeOptions(resourceType);
    if (resourceTypeOptions != null) {
      resourceName = resourceTypeOptions.getResourceTypeUnit();
    }
    return resourceName;
  }

  private static ResourceTypeOptions getResourceTypeOptions(ResourceType resourceType) {
    ProtocolMessageEnum resourceEnum;
    if (resourceType == null) {
      LOG.error("resourceType is null");
      return null;
    }
    if (resourceType.getResourceTypeOneofCase() == ResourceTypeOneofCase.JVM) {
      resourceEnum = resourceType.getJVM();
    }
    else if (resourceType.getResourceTypeOneofCase() == ResourceTypeOneofCase.HARDWARE_RESOURCE_TYPE) {
      resourceEnum = resourceType.getHardwareResourceType();
    }
    else if (resourceType.getResourceTypeOneofCase() == ResourceTypeOneofCase.THREADPOOL) {
      resourceEnum = resourceType.getThreadpool();
    }
    else {
      LOG.error("unknown resource enum type");
      return null;
    }
    return resourceEnum.getValueDescriptor().getOptions()
        .getExtension(PANetworking.resourceTypeOptions);
  }

  @VisibleForTesting
  static ResourceTypeOptions getResourceTypeOptions(ProtocolMessageEnum resourceEnum) {
    return resourceEnum.getValueDescriptor().getOptions()
        .getExtension(PANetworking.resourceTypeOptions);
  }

  /**
   * Map resourceTypeName to its enum object
   * @param resourceTypeName The resourceTypeName field defined in protbuf.
   * @return ResourceType enum object
   */
  @Nullable
  public static ResourceType buildResourceType(String resourceTypeName) {
    ResourceType resourceType = null;
    if (resourceTypeName == null) {
      return resourceType;
    }
    ResourceType.Builder builder = null;
    if (resourceTypeName.equals(ResourceTypeUtil.getResourceTypeOptions(JvmEnum.OLD_GEN).getResourceTypeName())) {
      builder = ResourceType.newBuilder().setJVM(JvmEnum.OLD_GEN);
    }
    else if (resourceTypeName.equals(ResourceTypeUtil.getResourceTypeOptions(JvmEnum.YOUNG_GEN).getResourceTypeName())) {
      builder = ResourceType.newBuilder().setJVM(JvmEnum.YOUNG_GEN);
    }
    else if (resourceTypeName.equals(ResourceTypeUtil.getResourceTypeOptions(HardwareEnum.CPU).getResourceTypeName())) {
      builder = ResourceType.newBuilder().setHardwareResourceType(HardwareEnum.CPU);
    }
    if (builder != null) {
      resourceType = builder.build();
    }
    return resourceType;
  }

}
