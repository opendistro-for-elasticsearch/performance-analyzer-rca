package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import java.nio.file.Paths;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class RcaUtilTest {

  @Test
  public void doTagsMatch() {
    Node node = new CPU_Utilization(5);
    node.addTag("locus", "data-node");
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());
    assertTrue(RcaUtil.doTagsMatch(node, rcaConf));
  }

  @Test
  public void noMatchWithExtraNodeTags() {
    Node node = new CPU_Utilization(5);
    node.addTag("locus", "data-node");
    // This is the extra tag.
    node.addTag("name", "sifi");
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());
    assertFalse(RcaUtil.doTagsMatch(node, rcaConf));
  }

  @Test
  public void noNodeTagsIsAMatch() {
    Node node = new CPU_Utilization(5);
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());
    assertTrue(RcaUtil.doTagsMatch(node, rcaConf));
  }

  @Test
  public void existingTagWithDifferentValueNoMatch() {
    Node node = new CPU_Utilization(5);
    node.addTag("locus", "master-node");
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());
    assertFalse(RcaUtil.doTagsMatch(node, rcaConf));
  }
}
