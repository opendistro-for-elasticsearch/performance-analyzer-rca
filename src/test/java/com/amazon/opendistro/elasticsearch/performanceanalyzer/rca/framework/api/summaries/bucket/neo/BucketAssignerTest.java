package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.neo;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.BucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.UsageBucket;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class BucketAssignerTest {
  @Test
  public void testCreateBucketAssigner() {
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "bucketization", "bucketize.json").toString());

    BucketCalculator bucketCalculatorForBase1 = rcaConf.getBucketizationSettings("base1");

    Assert.assertEquals(UsageBucket.UNDER_UTILIZED, bucketCalculatorForBase1.compute(19));
    Assert.assertEquals(UsageBucket.HEALTHY_WITH_BUFFER, bucketCalculatorForBase1.compute(40));
    Assert.assertEquals(UsageBucket.HEALTHY, bucketCalculatorForBase1.compute(80));
    Assert.assertEquals(UsageBucket.UNHEALTHY, bucketCalculatorForBase1.compute(81));

    BucketCalculator bucketCalculatorForBase3 = rcaConf.getBucketizationSettings("base3");

    Assert.assertEquals(UsageBucket.UNDER_UTILIZED, bucketCalculatorForBase3.compute(30));
    Assert.assertEquals(UsageBucket.HEALTHY_WITH_BUFFER, bucketCalculatorForBase3.compute(40.1));
    Assert.assertEquals(UsageBucket.HEALTHY, bucketCalculatorForBase3.compute(75));
    Assert.assertEquals(UsageBucket.UNHEALTHY, bucketCalculatorForBase3.compute(76));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMisSpelledTunableName() {
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "bucketization", "bucketize.json").toString());
    rcaConf.getBucketizationSettings("basa3");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoBucketizationSettings() {
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());
    rcaConf.getBucketizationSettings("base1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoThresholds() {
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "bucketization", "bucketize_no_thresholds.json").toString());
    rcaConf.getBucketizationSettings("base1");
  }
}