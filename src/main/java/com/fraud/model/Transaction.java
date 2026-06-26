package com.fraud.model;

import java.io.Serializable;
import java.util.Locale;

/**
 * Data model for a single credit-card transaction.
 *
 * Design notes:
 * - This is a valid Flink POJO: public no-arg constructor + public fields.
 *   That lets Flink serialize it efficiently between operators without
 *   any extra configuration.
 * - The same class is shared by the simulator layer (which serializes it
 *   to JSON) and the processing layer (which deserializes it back),
 *   keeping a single source of truth for the event schema.
 * - JSON field names use snake_case exactly as required by the project spec.
 */
public class Transaction implements Serializable {

    private static final long serialVersionUID = 1L;

    public String transactionId;
    public String cardId;
    public String customerId;
    public String merchantId;
    public String merchantCategory;
    public String country;
    public long eventTime;   // epoch milliseconds
    public double amount;

    /** Required by Flink POJO serialization and by Jackson. */
    public Transaction() {
    }

    public Transaction(String transactionId, String cardId, String customerId,
                       String merchantId, String merchantCategory,
                       String country, long eventTime, double amount) {
        this.transactionId = transactionId;
        this.cardId = cardId;
        this.customerId = customerId;
        this.merchantId = merchantId;
        this.merchantCategory = merchantCategory;
        this.country = country;
        this.eventTime = eventTime;
        this.amount = amount;
    }

    /**
     * Serializes the transaction to a single-line JSON string with the exact
     * field names mandated by the project specification.
     *
     * Built manually (no JSON library) so the simulator module stays
     * 100% dependency-free and can run with a plain `java` command.
     * Locale.US guarantees a dot decimal separator regardless of the
     * machine's locale settings.
     */
    public String toJson() {
        return String.format(Locale.US,
                "{\"transaction_id\":\"%s\",\"card_id\":\"%s\",\"customer_id\":\"%s\","
                        + "\"merchant_id\":\"%s\",\"merchant_category\":\"%s\","
                        + "\"country\":\"%s\",\"event_time\":%d,\"amount\":%.2f}",
                transactionId, cardId, customerId, merchantId,
                merchantCategory, country, eventTime, amount);
    }

    @Override
    public String toString() {
        return toJson();
    }
}
