package com.fraud.processing.metrics;

import java.io.Serializable;

/**
 * Tiny accumulator holding a running count and a running sum.
 *
 * This single accumulator powers ALL THREE required metrics:
 *   Metric 1 (per-minute totals)      -> uses count + sum
 *   Metric 2 (5-min category average) -> uses sum / count
 *   Metric 3 (5-min country count)    -> uses count only
 *
 * Keeping only two numbers per window (instead of buffering every
 * transaction) is what makes incremental window aggregation memory-safe
 * on an unbounded stream.
 */
public class CountSumAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    public long count;
    public double sum;

    public CountSumAccumulator() {
    }

    public CountSumAccumulator(long count, double sum) {
        this.count = count;
        this.sum = sum;
    }
}
