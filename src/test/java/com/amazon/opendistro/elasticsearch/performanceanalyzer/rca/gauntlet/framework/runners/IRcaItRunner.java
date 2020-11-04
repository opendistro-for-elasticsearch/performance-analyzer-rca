package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.framework.runners;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.framework.Cluster;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.framework.configs.ClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.framework.configs.Consts;
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
