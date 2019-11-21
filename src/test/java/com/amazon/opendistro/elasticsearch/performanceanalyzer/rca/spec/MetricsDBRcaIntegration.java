package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.helpers.AssertHelper;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class MetricsDBRcaIntegration {
  @Test
  public void testIntegration() throws Exception {
    Queryable queryable = new MetricsDBProviderTestHelper();
    List<List<String>> expectedReturn = new ArrayList<>();
    List<String> cols =
        new ArrayList<String>() {
          {
            this.add("ShardID");
            this.add("IndexName");
            this.add("Operation");
            this.add("ShardRole");
            this.add("sum");
            this.add("avg");
            this.add("min");
            this.add("max");
          }
        };
    List<String> row1 =
        new ArrayList<String>() {
          {
            this.add("CPU_UTILIZATIONShardID");
            this.add("CPU_UTILIZATIONIndexName");
            this.add("CPU_UTILIZATIONOperation");
            this.add("CPU_UTILIZATIONShardRole");
            this.add("1.0");
            this.add("1.0");
            this.add("1.0");
            this.add("1.0");
          }
        };

    expectedReturn.add(cols);
    expectedReturn.add(row1);

    int idx = 0;
    MetricsDB db = queryable.getMetricsDB();
    for (List<String> row :
        queryable.queryMetrics(db, AllMetrics.OSMetrics.CPU_UTILIZATION.name())) {
      AssertHelper.compareLists(expectedReturn.get(idx), row);
      ++idx;
    }
  }
}
