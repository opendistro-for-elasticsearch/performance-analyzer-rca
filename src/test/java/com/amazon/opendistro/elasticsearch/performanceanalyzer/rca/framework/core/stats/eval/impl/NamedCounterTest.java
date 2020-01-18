package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.NamedAggregateValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.Value;
import org.junit.Assert;
import org.junit.Test;

public class NamedCounterTest {

  @Test
  public void calculate() {
    NamedCounter namedCounter = new NamedCounter();
    namedCounter.calculate("x", 20);
    namedCounter.calculate("x", 20);
    namedCounter.calculate("x", 20);
    namedCounter.calculate("y", 20);
    namedCounter.calculate("y", 20);
    namedCounter.calculate("z", 20);

    for (NamedAggregateValue v : namedCounter.get()) {
      if (v.getName().equals("x")) {
        Assert.assertEquals(3L, v.getValue());
      } else if (v.getName().equals("y")) {
        Assert.assertEquals(2L, v.getValue());
      } else if (v.getName().equals("z")) {
        Assert.assertEquals(1L, v.getValue());
      }
    }
  }

  @Test
  public void concurrentCalculate() {
    int N = 2000000;
    int countOfEach = 5000;

    int differentKeys = N / countOfEach;
    String[] arr = new String[N];

    for (int i = 0; i < differentKeys; i++) {
      String name = "x" + i;
      for (int j = i; j < N; j += differentKeys) {
        arr[j] = name;
      }
    }

    NamedCounter namedCounter = new NamedCounter();
    Th[] threads = new Th[countOfEach];

    int thi = 0;
    for (int i = 0; i < N; i += differentKeys, thi++) {
      threads[thi] = new Th(arr, i, differentKeys, namedCounter);
    }

    for (int i=0; i<thi; i++) {
      threads[i].start();
    }

    for (int i=0; i<thi; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    for (NamedAggregateValue value: namedCounter.get()) {
      boolean found = false;
      for (int i = 0; i < differentKeys; i++) {
        String name = "x" + i;
        if (name.equals(value.getName())) {
          Assert.assertEquals((long)countOfEach, value.getValue());
          found = true;
          break;
        }
      }
      Assert.assertTrue(found);
    }
  }

  class Th extends Thread {
    String[] arr;
    int start;
    int delta;
    NamedCounter namedCounter;

    Th(String[] arr, int start, int delta, NamedCounter counter) {
      this.arr = arr;
      this.start = start;
      this.delta = delta;
      this.namedCounter = counter;
    }

    public void run() {
      for (int i = start; i < start + delta; i++) {
        namedCounter.calculate(arr[i], 0);
        // System.out.println(Thread.currentThread().getId() + ": "+arr[i]);
      }
    }
  }
}
