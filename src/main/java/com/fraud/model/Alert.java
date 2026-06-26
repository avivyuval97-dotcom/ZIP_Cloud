package com.fraud.model;

import java.io.Serializable;
import java.util.Locale;

/**
 * Data model for an anomaly alert (Parts C-E of the project).
 *
 * Carries everything the spec requires an alert to contain:
 *   - event time of the suspicious transaction
 *   - the relevant entity (Card / Customer / Merchant) and its id
 *   - the rule that fired
 *   - the values used for the decision (window count, historical avg,
 *     countries and time gap, ...) in the decisionValues field
 *   - the full suspicious Transaction object (for risk scoring and for
 *     printing its details)
 *
 * Bonus (Part E): riskScore (0-100) and riskLevel (LOW/MEDIUM/HIGH) are
 * filled in by the RiskScorer stage after the alert is created by a rule.
 *
 * Like Transaction, this is a valid Flink POJO (public fields + no-arg
 * constructor) so alert streams flow between operators efficiently.
 */
public class Alert implements Serializable {

    private static final long serialVersionUID = 1L;

    public long timestamp;
    public String entityType;     // "Card", "Customer" or "Merchant"
    public String entityId;
    public String rule;           // e.g. "High Frequency"
    public String decisionValues; // rule-specific, e.g. "Transactions in 2 minutes: 7"
    public Transaction tx;        // the suspicious transaction itself

    // Filled by the risk-scoring stage (bonus). Empty level = not scored.
    public int riskScore;
    public String riskLevel = "";

    public Alert() {
    }

    public Alert(long timestamp, String entityType, String entityId,
                 String rule, String decisionValues, Transaction tx) {
        this.timestamp = timestamp;
        this.entityType = entityType;
        this.entityId = entityId;
        this.rule = rule;
        this.decisionValues = decisionValues;
        this.tx = tx;
    }

    /** Compact human-readable summary of a transaction, used in alerts. */
    public static String describe(Transaction tx) {
        if (tx == null) {
            return "n/a";
        }
        return String.format(Locale.US, "%s, merchant %s (%s), country %s, amount %.2f",
                tx.transactionId, tx.merchantId, tx.merchantCategory,
                tx.country, tx.amount);
    }

    /**
     * Formats the alert exactly in the structure suggested by the spec,
     * followed by the risk classification (bonus) and the suspicious
     * transaction details (also required).
     */
    @Override
    public String toString() {
        String risk = riskLevel.isEmpty()
                ? ""
                : String.format(Locale.US, " | Risk Score: %d/100 (%s)", riskScore, riskLevel);
        return String.format(Locale.US,
                "Anomaly Detected! Timestamp: %d, %s: %s, Rule: %s, %s%s | Suspicious Tx: %s",
                timestamp, entityType, entityId, rule, decisionValues,
                risk, describe(tx));
    }
}
