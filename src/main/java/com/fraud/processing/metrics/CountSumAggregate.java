package com.fraud.processing.metrics;

import com.fraud.model.Transaction;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental window aggregation: updates a (count, sum) accumulator for
 * every transaction the moment it arrives, instead of buffering all
 * window elements and computing at window close.
 *
 * Why this matters (classic Flink pattern):
 * - O(1) state per window regardless of traffic volume
 * - The window fires instantly when it closes - the math is already done
 *
 * merge() is required by the interface to support merging window state
 * (e.g. for session windows); for tumbling windows it is rarely invoked
 * but must still be correct.
 */
public class CountSumAggregate
        implements AggregateFunction<Transaction, CountSumAccumulator, CountSumAccumulator> {

    private static final long serialVersionUID = 1L;

    @Override
    public CountSumAccumulator createAccumulator() {
        return new CountSumAccumulator(0L, 0.0);
    }

    @Override
    public CountSumAccumulator add(Transaction tx, CountSumAccumulator acc) {
        acc.count += 1;
        acc.sum += tx.amount;
        return acc;
    }

    @Override
    public CountSumAccumulator getResult(CountSumAccumulator acc) {
        return acc;
    }

    @Override
    public CountSumAccumulator merge(CountSumAccumulator a, CountSumAccumulator b) {
        return new CountSumAccumulator(a.count + b.count, a.sum + b.sum);
    }
}
