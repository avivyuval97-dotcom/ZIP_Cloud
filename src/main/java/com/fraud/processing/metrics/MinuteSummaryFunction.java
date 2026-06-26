package com.fraud.processing.metrics;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * Metric 1: total transaction count and total amount per 1-minute window.
 *
 * This is a GLOBAL (non-keyed) metric, hence ProcessAllWindowFunction.
 * It runs as the second stage of an incremental aggregation: it receives
 * exactly one pre-computed CountSumAccumulator from CountSumAggregate
 * and only attaches the window-end timestamp and the required formatting.
 *
 * Output format (as mandated by the spec):
 *   Timestamp: 1715606460000, Total Transactions: 854, Total Amount: 428930.70
 */
public class MinuteSummaryFunction
        extends ProcessAllWindowFunction<CountSumAccumulator, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(Context context,
                        Iterable<CountSumAccumulator> elements,
                        Collector<String> out) {
        CountSumAccumulator acc = elements.iterator().next();
        out.collect(String.format(Locale.US,
                "Timestamp: %d, Total Transactions: %d, Total Amount: %.2f",
                context.window().getEnd(), acc.count, acc.sum));
    }
}
