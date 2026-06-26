package com.fraud.processing.rules;

import com.fraud.model.Alert;
import com.fraud.model.Transaction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Rule 1 - High Frequency:
 * more than 5 transactions for the same card_id within a 2-minute window.
 *
 * Implementation choice: this rule is a textbook WINDOW problem
 * ("count events inside a fixed time slice"), so it is implemented with
 * keyBy(card_id) + a 2-minute tumbling window + incremental aggregation.
 * The alert is emitted when the window closes, if the count exceeded
 * the threshold.
 *
 * The accumulator keeps the count AND the most recent transaction, so the
 * alert can carry the suspicious transaction's details and event time
 * as the spec requires.
 */
public class HighFrequencyRule {

    /** Strictly more than 5 transactions per 2 minutes triggers an alert. */
    public static final int MAX_TX_PER_WINDOW = 5;

    /** Accumulator: running count + latest transaction seen in the window. */
    public static class FreqAccumulator implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long count;
        public Transaction lastTx;

        public FreqAccumulator() {
        }
    }

    /** Incremental count per (card, window). */
    public static class FreqAggregate
            implements AggregateFunction<Transaction, FreqAccumulator, FreqAccumulator> {
        private static final long serialVersionUID = 1L;

        @Override
        public FreqAccumulator createAccumulator() {
            return new FreqAccumulator();
        }

        @Override
        public FreqAccumulator add(Transaction tx, FreqAccumulator acc) {
            acc.count += 1;
            acc.lastTx = tx;
            return acc;
        }

        @Override
        public FreqAccumulator getResult(FreqAccumulator acc) {
            return acc;
        }

        @Override
        public FreqAccumulator merge(FreqAccumulator a, FreqAccumulator b) {
            FreqAccumulator merged = new FreqAccumulator();
            merged.count = a.count + b.count;
            // keep the more recent of the two transactions
            merged.lastTx = (b.lastTx != null) ? b.lastTx : a.lastTx;
            return merged;
        }
    }

    /** Fires an alert only when the per-card window count exceeds the limit. */
    public static class FreqAlertFunction
            extends ProcessWindowFunction<FreqAccumulator, Alert, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(String cardId,
                            Context context,
                            Iterable<FreqAccumulator> elements,
                            Collector<Alert> out) {
            FreqAccumulator acc = elements.iterator().next();
            if (acc.count > MAX_TX_PER_WINDOW && acc.lastTx != null) {
                out.collect(new Alert(
                        acc.lastTx.eventTime,
                        "Card",
                        cardId,
                        "High Frequency",
                        "Transactions in 2 minutes: " + acc.count,
                        acc.lastTx));
            }
            // count <= threshold: emit nothing - normal traffic.
        }
    }
}
