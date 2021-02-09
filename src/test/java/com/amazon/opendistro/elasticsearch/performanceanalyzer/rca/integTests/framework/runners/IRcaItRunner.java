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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.runners;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.Cluster;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.ClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.Consts;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import org.apache.commons.io.FileUtils;

public interface IRcaItRunner {
  String SET_CLUSTER_METHOD = "setTestApi";

  default Cluster createCluster(ClusterType clusterType, boolean useHttps) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat(Consts.RCA_IT_CLUSTER_DIR_FORMAT);
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    String formattedTime = sdf.format(timestamp);
    Path rcaItDir = Paths.get(Consts.RCA_IT_BASE_DIR, formattedTime);

    cleanUpDirsFromLastRuns();
    createITDirForThisRun(rcaItDir);

    return new Cluster(clusterType, rcaItDir.toFile(), useHttps);
  }

  default void cleanUpDirsFromLastRuns() throws IOException {
    FileUtils.deleteDirectory(Paths.get(Consts.RCA_IT_BASE_DIR).toFile());
  }

  default void createITDirForThisRun(Path rcaItDir) {
    File clusterDir = rcaItDir.toFile();
    File parent = clusterDir.getParentFile();
    if (!clusterDir.exists() && !clusterDir.mkdirs()) {
      throw new IllegalStateException("Couldn't create dir: " + parent);
    }
  }
}
