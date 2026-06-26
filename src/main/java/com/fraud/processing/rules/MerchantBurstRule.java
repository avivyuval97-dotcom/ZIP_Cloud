package com.fraud.processing.rules;

import com.fraud.model.Alert;
import com.fraud.model.Transaction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Rule 4 (BONUS) - Merchant Burst:
 * more than 20 transactions for the same merchant_id within a 2-minute
 * window. This pattern indicates an attack on a single merchant (e.g.
 * card-testing fraud, where stolen cards are validated with many small
 * purchases at one vulnerable merchant).
 *
 * Calibration: at the default configuration (120 tx/min, 30 merchants,
 * two spending tiers) a normal merchant sees ~9 transactions per
 * 2-minute window, so the threshold of 20 is far above organic load and
 * is only crossed by injected bursts (15-20 extra transactions).
 *
 * Known limitation (worth explaining): a tumbling window can split a
 * burst that lands exactly on a window boundary, occasionally missing a
 * detection. A sliding window would fix this at the cost of duplicate
 * alerts - a classic streaming trade-off.
 */
public class MerchantBurstRule {

    /** Strictly more than 20 transactions per 2 minutes triggers an alert. */
    public static final int MAX_TX_PER_WINDOW = 20;

    /** Accumulator: running count + latest transaction seen in the window. */
    public static class BurstAccumulator implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public long count;
        public Transaction lastTx;

        public BurstAccumulator() {
        }
    }

    /** Incremental count per (merchant, window). */
    public static class BurstAggregate
            implements AggregateFunction<Transaction, BurstAccumulator, BurstAccumulator> {
        private static final long serialVersionUID = 1L;

        @Override
        public BurstAccumulator createAccumulator() {
            return new BurstAccumulator();
        }

        @Override
        public BurstAccumulator add(Transaction tx, BurstAccumulator acc) {
            acc.count += 1;
            acc.lastTx = tx;
            return acc;
        }

        @Override
        public BurstAccumulator getResult(BurstAccumulator acc) {
            return acc;
        }

        @Override
        public BurstAccumulator merge(BurstAccumulator a, BurstAccumulator b) {
            BurstAccumulator merged = new BurstAccumulator();
            merged.count = a.count + b.count;
            merged.lastTx = (b.lastTx != null) ? b.lastTx : a.lastTx;
            return merged;
        }
    }

    /** Fires an alert only when the per-merchant window count exceeds the limit. */
    public static class BurstAlertFunction
            extends ProcessWindowFunction<BurstAccumulator, Alert, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(String merchantId,
                            Context context,
                            Iterable<BurstAccumulator> elements,
                            Collector<Alert> out) {
            BurstAccumulator acc = elements.iterator().next();
            if (acc.count > MAX_TX_PER_WINDOW && acc.lastTx != null) {
                out.collect(new Alert(
                        acc.lastTx.eventTime,
                        "Merchant",
                        merchantId,
                        "Merchant Burst",
                        "Transactions in 2 minutes: " + acc.count
                                + " (normal load ~9)",
                        acc.lastTx));
            }
        }
    }
}
