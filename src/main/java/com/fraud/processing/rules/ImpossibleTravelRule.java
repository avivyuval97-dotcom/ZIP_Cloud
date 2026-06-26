package com.fraud.processing.rules;

import com.fraud.model.Alert;
import com.fraud.model.Transaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Rule 3 - Impossible Travel:
 * two transactions for the same card_id in two different countries,
 * less than 30 minutes apart.
 *
 * Implementation choice: like Rule 2, this compares an event to the
 * card's PREVIOUS event, not to a time slice - so it uses keyed state
 * rather than a window. A single ValueState per card remembers where
 * and when the card was last used; every incoming transaction is checked
 * against it instantly and then becomes the new "last location".
 *
 * This is the minimal possible state (one small object per card),
 * demonstrating that keyed state is chosen per problem - ListState for
 * rolling history (Rule 2), ValueState for "remember the last thing"
 * (this rule).
 */
public class ImpossibleTravelRule
        extends KeyedProcessFunction<String, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    public static final long MAX_GAP_MS = 30 * 60 * 1000L; // 30 minutes

    /** Small POJO stored in state: where and when the card was last seen. */
    public static class LastLocation implements Serializable {
        private static final long serialVersionUID = 1L;
        public String country;
        public long eventTime;

        public LastLocation() {
        }

        public LastLocation(String country, long eventTime) {
            this.country = country;
            this.eventTime = eventTime;
        }
    }

    /** Keyed state: the previous (country, time) per card. */
    private transient ValueState<LastLocation> lastLocation;

    @Override
    public void open(Configuration parameters) {
        lastLocation = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-location", LastLocation.class));
    }

    @Override
    public void processElement(Transaction tx,
                               Context ctx,
                               Collector<Alert> out) throws Exception {
        LastLocation previous = lastLocation.value();

        if (previous != null
                && !previous.country.equals(tx.country)
                && tx.eventTime - previous.eventTime < MAX_GAP_MS) {

            long gapMinutes = (tx.eventTime - previous.eventTime) / 60_000L;
            out.collect(new Alert(
                    tx.eventTime,
                    "Card",
                    tx.cardId,
                    "Impossible Travel",
                    String.format(
                            "Previous Country: %s, Current Country: %s, Time Gap Minutes: %d",
                            previous.country, tx.country, gapMinutes),
                    tx));
        }

        // The current transaction always becomes the new reference point.
        lastLocation.update(new LastLocation(tx.country, tx.eventTime));
    }
}
