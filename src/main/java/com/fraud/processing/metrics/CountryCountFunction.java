package com.fraud.processing.metrics;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * Metric 3: number of transactions per country,
 * over 5-minute tumbling windows.
 *
 * Keyed by country; reuses the same CountSumAccumulator and simply
 * ignores the sum component (only the count is needed here).
 *
 * Output format (as mandated by the spec):
 *   Timestamp: 1715606700000, Country: IL, Transactions: 497
 */
public class CountryCountFunction
        extends ProcessWindowFunction<CountSumAccumulator, String, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String country,
                        Context context,
                        Iterable<CountSumAccumulator> elements,
                        Collector<String> out) {
        CountSumAccumulator acc = elements.iterator().next();
        out.collect(String.format(Locale.US,
                "Timestamp: %d, Country: %s, Transactions: %d",
                context.window().getEnd(), country, acc.count));
    }
}
