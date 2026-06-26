package com.fraud.processing.metrics;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * Metric 2: average transaction amount per merchant_category,
 * over 5-minute tumbling windows.
 *
 * This is a KEYED metric: the stream is keyBy(merchant_category) before
 * windowing, so Flink maintains an independent window (and accumulator)
 * for every category in parallel. The function receives the category as
 * the window key and computes average = sum / count.
 *
 * Output format (as mandated by the spec):
 *   Timestamp: 1715606700000, Category: electronics, Average Amount: 611.35
 */
public class CategoryAverageFunction
        extends ProcessWindowFunction<CountSumAccumulator, String, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String category,
                        Context context,
                        Iterable<CountSumAccumulator> elements,
                        Collector<String> out) {
        CountSumAccumulator acc = elements.iterator().next();
        double average = (acc.count == 0) ? 0.0 : acc.sum / acc.count;
        out.collect(String.format(Locale.US,
                "Timestamp: %d, Category: %s, Average Amount: %.2f",
                context.window().getEnd(), category, average));
    }
}
