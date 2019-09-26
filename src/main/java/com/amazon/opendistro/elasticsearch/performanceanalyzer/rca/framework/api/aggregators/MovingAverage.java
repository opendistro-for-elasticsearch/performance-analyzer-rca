package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators;

/**
 * To calculate the sliding window moving average.
 *
 * <p>So for a window size of 3: [1] : 1 [1, 10] : (1 + 10) / 2 [1, 10, 23] : (1 + 10 + 23) / 3 [1,
 * 10, 23, 33] : (10 + 23 + 33) / 3
 */
public class MovingAverage {
  private double sum;
  private int oldestElementIndex;
  private double[] lastNElements;
  private boolean isArrayFull;

  public MovingAverage(int window_length) {
    this.lastNElements = new double[window_length];
    this.oldestElementIndex = 0;
    this.sum = 0;
    this.isArrayFull = false;
  }

  /**
   * Takes the next input to the Average calculator.
   *
   * @param val Next value in the stream.
   * @return The average so far.
   */
  public double next(double val) {
    sum -= lastNElements[oldestElementIndex];
    sum += val;
    lastNElements[oldestElementIndex] = val;
    oldestElementIndex = (oldestElementIndex + 1) % lastNElements.length;
    if (!isArrayFull && oldestElementIndex == 0) {
      isArrayFull = true;
    }
    int count = lastNElements.length;
    if (!isArrayFull) {
      count = oldestElementIndex;
      return -1.0;
    }
    return (sum * 1.0f / count);
  }
}
