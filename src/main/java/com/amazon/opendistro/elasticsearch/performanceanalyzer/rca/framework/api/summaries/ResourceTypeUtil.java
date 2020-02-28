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
import com.google.protobuf.ProtocolMessageEnum;

/**
 * A utility class to parse and build grpc ResourceType
 */
public class ResourceTypeUtil {

  /**
   * Read the resourceType name from the ResourceType object
   * @param resourceType grpc ResourceType object
   * @return resource type name
   */
  public static String getResourceTypeName(ResourceType resourceType) {
    String resourceName = "unknown resource type";
    if (resourceType != null) {
      if (resourceType.getResourceTypeOneofCase() == ResourceTypeOneofCase.JVM) {
        resourceName = ResourceTypeUtil.getResourceTypeName(resourceType.getJVM());
      }
      else if (resourceType.getResourceTypeOneofCase() == ResourceTypeOneofCase.HARDWARE_RESOURCE_TYPE) {
        resourceName = ResourceTypeUtil.getResourceTypeName(resourceType.getHardwareResourceType());
      }
    }
    return resourceName;
  }

  private static String getResourceTypeName(ProtocolMessageEnum resourceEnum) {
    return resourceEnum.getValueDescriptor().getOptions()
        .getExtension(PANetworking.resourceTypeName);
  }

  /**
   * Map resourceTypeName to its enum object
   * @param resourceTypeName The resourceTypeName field defined in protbuf.
   * @return ResourceType enum object
   */
  public static ResourceType buildResourceType(String resourceTypeName) {
    ResourceType resourceType = null;
    if (resourceTypeName == null) {
      return resourceType;
    }
    ResourceType.Builder builder = null;
    if (resourceTypeName.equals(ResourceTypeUtil.getResourceTypeName(JvmEnum.OLD_GEN))) {
      builder = ResourceType.newBuilder().setJVM(JvmEnum.OLD_GEN);
    }
    else if (resourceTypeName.equals(ResourceTypeUtil.getResourceTypeName(JvmEnum.YOUNG_GEN))) {
      builder = ResourceType.newBuilder().setJVM(JvmEnum.YOUNG_GEN);
    }
    else if (resourceTypeName.equals(ResourceTypeUtil.getResourceTypeName(HardwareEnum.CPU))) {
      builder = ResourceType.newBuilder().setHardwareResourceType(HardwareEnum.CPU);
    }
    if (builder != null) {
      resourceType = builder.build();
    }
    return resourceType;
  }

}
