package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.old_gen;

public class OldGenDecisionUtils {
  /**
   * This function divide the range {lower bound - upper bound } of search/wrire queue into
   * buckets. And allocate the val into its corresponding bucket. The value here refers to the
   * EWMA size of search/write queue. step here is calculated as {range of queue} / {num of buckets}
   * The queue's lower/upper bound can be configured in rca.conf
   */
  public static int bucketization(int lowerBound, int upperBound, int val, int bucketSize) {
    double step = (double) (upperBound - lowerBound) / (double) bucketSize;
    return (int) ((double) val / step);
  }

  /**
   * this function calculate the size of each step given the range {lower bound - upper bound}
   * and number of steps
   * @param lowerBound lowerbound
   * @param upperBound upperbound
   * @param stepCount number of steps
   * @return size of a single step
   */
  public static double calculateStepSize(double lowerBound, double upperBound, int stepCount) {
    return (upperBound - lowerBound) / (double) stepCount;
  }

  public static int calculateStepSize(int lowerBound, int upperBound, int stepCount) {
    double step = (double) (upperBound - lowerBound) / (double) stepCount;
    return (int)step;
  }
}
