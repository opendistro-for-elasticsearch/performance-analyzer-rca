package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class SlidingWindowTest {

  @Test
  public void testSlidingWindow() {
    SlidingWindow slidingWindow = new SlidingWindow(3);

    // if the sliding window is empty, return NaN
    Assert.assertTrue(Double.isNaN(slidingWindow.read()));

    // only one element in window, return NaN
    slidingWindow.next(0, 1);
    Assert.assertTrue(Double.isNaN(slidingWindow.read()));

    // 2nd - 4th element
    slidingWindow.next(TimeUnit.SECONDS.toMillis(1), 10);
    Assert.assertEquals(11, slidingWindow.read(), 0.1);

    slidingWindow.next(TimeUnit.SECONDS.toMillis(2), 3);
    Assert.assertEquals(7, slidingWindow.read(), 0.1);

    slidingWindow.next(TimeUnit.SECONDS.toMillis(3), 5);
    Assert.assertEquals(6.333, slidingWindow.read(), 0.1);

    //5th element, the sliding window starts to pop out old ones
    slidingWindow.next(TimeUnit.SECONDS.toMillis(4), 7);
    Assert.assertEquals(8.333, slidingWindow.read(), 0.1);
  }
}
