package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class MovingAverageTest {

    @Test
    public void next() {
        MovingAverage avg = new MovingAverage(3);

        // 1 element is below the window size limit, so the average returns -1.
        Assert.assertEquals(-1, avg.next(1), 0.1);

        // 2 elements are below the window size limit, so the average returns -1.
        Assert.assertEquals(-1, avg.next(10), 0.1);

        // 3 elements is equal to the window size limit, so the average returns the expected value.
        Assert.assertEquals((1 + 10 + 3)/3.0, avg.next(3), 0.1);
        Assert.assertEquals((10 + 3 + 5)/3.0, avg.next(5), 0.1);
        Assert.assertEquals((3 + 5 + 7)/3.0, avg.next(7), 0.1);
    }
}