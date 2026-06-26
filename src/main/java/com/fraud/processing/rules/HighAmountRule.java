package com.fraud.processing.rules;

import com.fraud.model.Alert;
import com.fraud.model.Transaction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Rule 2 - High Amount:
 * a transaction whose amount is at least 3x the average of the customer's
 * last 20 transactions, AND greater than 500 currency units.
 *
 * Implementation choice: this is NOT a window problem - it compares each
 * incoming event against the customer's rolling HISTORY. The natural tool
 * is a KeyedProcessFunction with keyed ListState:
 *   - the stream is keyBy(customer_id), so Flink maintains a completely
 *     separate state instance per customer, transparently
 *   - the state holds the amounts of the customer's last 20 transactions
 *   - every new transaction is checked the moment it arrives (real-time
 *     alerting, no window delay), and then appended to the history
 *
 * Design decision (cold start): a brand-new customer with 1-2 historical
 * transactions has a statistically meaningless average, which would cause
 * false alerts. The rule therefore activates only after MIN_HISTORY
 * transactions exist for the customer.
 *
 * Important ordering: the check happens BEFORE the new amount is added to
 * the history, so a fraudulent spike cannot "dilute" its own baseline.
 */
public class HighAmountRule
        extends KeyedProcessFunction<String, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    public static final int HISTORY_SIZE = 20;
    public static final int MIN_HISTORY = 5;
    public static final double RATIO_THRESHOLD = 3.0;
    public static final double MIN_AMOUNT = 500.0;

    /** Keyed state: amounts of the last HISTORY_SIZE transactions per customer. */
    private transient ListState<Double> recentAmounts;

    @Override
    public void open(Configuration parameters) {
        recentAmounts = getRuntimeContext().getListState(
                new ListStateDescriptor<>("recent-amounts", Double.class));
    }

    @Override
    public void processElement(Transaction tx,
                               Context ctx,
                               Collector<Alert> out) throws Exception {
        // 1. Read the customer's history (state is scoped to the current key).
        List<Double> history = new ArrayList<>();
        for (Double amount : recentAmounts.get()) {
            history.add(amount);
        }

        // 2. Check the rule against the history BEFORE adding the new amount.
        if (history.size() >= MIN_HISTORY) {
            double avg = history.stream()
                    .mapToDouble(Double::doubleValue).average().orElse(0.0);
            if (tx.amount >= RATIO_THRESHOLD * avg && tx.amount > MIN_AMOUNT) {
                out.collect(new Alert(
                        tx.eventTime,
                        "Customer",
                        tx.customerId,
                        "High Amount",
                        String.format(Locale.US,
                                "Amount: %.2f, Historical Avg: %.2f",
                                tx.amount, avg),
                        tx));
            }
        }

        // 3. Update the rolling history: append and trim to the last 20.
        history.add(tx.amount);
        if (history.size() > HISTORY_SIZE) {
            history = history.subList(history.size() - HISTORY_SIZE, history.size());
        }
        recentAmounts.update(history);
    }
}
