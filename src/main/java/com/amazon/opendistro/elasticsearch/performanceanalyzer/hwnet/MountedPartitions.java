/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.hwnet;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MountedPartitionMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.MountedPartitionMetricsGenerator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.linux.LinuxMountedPartitionMetricsGenerator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.SchemaFileParser;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.SchemaFileParser.FieldTypes;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MountedPartitions {

  private static final String CGROUP = "cgroup";
  private static final String PROC = "proc";
  private static final String SYSFS = "sysfs";
  private static final String DEV_PTS = "devpts";
  private static final String DEV_TMPFS = "devtmpfs";
  private static final String PATH_SYS = "/sys/";
  private static final String PATH_ETC = "/etc/";
  private static final String KEY_DEVICE_PARTITION = "devicePartition";
  private static final String KEY_MOUNT_POINT = "mountPoint";
  private static final String KEY_FILE_SYSTEM_TYPE = "fileSystemType";
  private static final String KEY_MOUNT_OPTIONS = "mountOptions";
  private static final String KEY_DUMP = "dump";
  private static final String KEY_PASS = "pass";
  private static final String NONE = "none";
  private static final LinuxMountedPartitionMetricsGenerator linuxMountedPartitionMetricsGenerator;
  private static final Map<String, File> mountPointFileMap;
  private static final Set<String> virtualSysPartitionSet;
  private static final Set<String> ignoredMountPoints;
  private static final String[] schemaKeys = {
      KEY_DEVICE_PARTITION,
      KEY_MOUNT_POINT,
      KEY_FILE_SYSTEM_TYPE,
      KEY_MOUNT_OPTIONS,
      KEY_DUMP,
      KEY_PASS
  };
  private static final FieldTypes[] schemaKeyTypes = {
      FieldTypes.STRING,  // devicePartition
      FieldTypes.STRING,  // mountPoint
      FieldTypes.STRING,  // fileSystemType
      FieldTypes.STRING,  // mountOptions
      FieldTypes.INT,     // dump
      FieldTypes.INT      // pass
  };

  static {
    linuxMountedPartitionMetricsGenerator = new LinuxMountedPartitionMetricsGenerator();
    mountPointFileMap = new HashMap<>();
    virtualSysPartitionSet = ImmutableSet.of(CGROUP, PROC, SYSFS, DEV_PTS, DEV_TMPFS, NONE);
    ignoredMountPoints = ImmutableSet.of(PATH_SYS, PATH_ETC);
  }

  public static void addSample() {
    SchemaFileParser parser = new SchemaFileParser("/proc/mounts", schemaKeys, schemaKeyTypes);
    List<Map<String, Object>> parsedMaps = parser.parseMultiple().stream().filter(map -> {
      String devicePartition = (String) map.get(KEY_DEVICE_PARTITION);
      String mountPoint = (String) map.get(KEY_MOUNT_POINT);

      return !(virtualSysPartitionSet.contains(devicePartition) || !devicePartition.startsWith(
          "/dev/") || ignoredMountPoints.contains(mountPoint));
    }).collect(Collectors.toList());
    for (Map<String, Object> mountInfo : parsedMaps) {
      String devicePartition = (String) mountInfo.get(KEY_DEVICE_PARTITION);
      String mountPoint = (String) mountInfo.get(KEY_MOUNT_POINT);

      long totalSpace = mountPointFileMap.computeIfAbsent(mountPoint, File::new).getTotalSpace();
      long freeSpace = mountPointFileMap.get(mountPoint).getFreeSpace();
      long usableFreeSpace = mountPointFileMap.get(mountPoint).getUsableSpace();
      MountedPartitionMetrics metrics = new MountedPartitionMetrics(devicePartition, mountPoint,
          totalSpace, freeSpace, usableFreeSpace);

      linuxMountedPartitionMetricsGenerator.addSupplier(mountPoint, metrics);
    }
  }

  public static MountedPartitionMetricsGenerator getLinuxMountedPartitionMetricsGenerator() {
    return linuxMountedPartitionMetricsGenerator;
  }
}
