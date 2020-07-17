package com.amazon.opendistro.elasticsearch.performanceanalyzer.collections;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TimeExpiringSetTest {
  private TimeExpiringSet<Integer> timeExpiringSet;

  @Before
  public void setup() {
    timeExpiringSet = new TimeExpiringSet<>(1, TimeUnit.SECONDS);
  }

  @Test
  public void testContains() throws Exception {
    timeExpiringSet.add(5);
    Thread.sleep(500L);
    Assert.assertTrue(timeExpiringSet.contains(5));
    Thread.sleep(600L);
    Assert.assertFalse(timeExpiringSet.contains(5));
  }

  @Test
  public void testElementRefresh() throws Exception {
    timeExpiringSet.add(5);
    Thread.sleep(500L);
    Assert.assertTrue(timeExpiringSet.contains(5));
    timeExpiringSet.add(5);
    Assert.assertEquals(1, timeExpiringSet.size());
    Thread.sleep(750L);
    Assert.assertTrue(timeExpiringSet.contains(5));
    Thread.sleep(300L);
    Assert.assertFalse(timeExpiringSet.contains(5));
  }

  /**
   * Verifies weakly-consistent iteration behavior
   */
  @Test
  public void testExpiringIteration() throws Exception {
    for (int i = 0; i < 100; i++) { // ~10s for perpetual sanity
      TimeExpiringSet<Integer> tes = new TimeExpiringSet<>(100, TimeUnit.MILLISECONDS);
      Set<Integer> seen = new HashSet<>();
      tes.add(5);
      Thread.sleep(50L);
      tes.add(10);
      Thread.sleep(30L);
      tes.add(15);
      Iterator<Integer> it = tes.iterator();
      Thread.sleep(50L);
      while (it.hasNext()) {
        seen.add(it.next());
      }
      Assert.assertTrue(seen.size() >= 2);
    }
  }
}
