package com.fraud.processing.risk;

import com.fraud.model.Alert;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Set;

/**
 * Risk scoring stage (BONUS - Part E).
 *
 * Every alert flowing toward the alert sink passes through this operator,
 * which evaluates the suspicious transaction and assigns:
 *   - riskScore: 0-100, additive model
 *   - riskLevel: LOW (<40) / MEDIUM (40-69) / HIGH (>=70)
 *
 * Scoring model (simple, explainable - a deliberate choice over an
 * opaque ML model for a rule-based system):
 *   base score by rule severity:
 *     Impossible Travel  60  (strongest fraud signal - physical impossibility)
 *     High Amount        50  (direct monetary risk)
 *     High Frequency     40  (card testing pattern)
 *     Merchant Burst     30  (merchant-side risk, not card-side)
 *   enrichment from EXTERNAL REFERENCE DATA (also bonus):
 *     +20 if the merchant appears in config/risky_merchants.txt
 *     +25 if the country appears in config/country_blacklist.txt
 *   transaction characteristics:
 *     +10 if the amount exceeds 1000
 *
 * Implemented as a RichMapFunction: the reference files are loaded ONCE
 * in open() (when the operator starts), not per event - the standard
 * pattern for static enrichment data.
 */
public class RiskScorer extends RichMapFunction<Alert, Alert> {

    private static final long serialVersionUID = 1L;

    private final String riskyMerchantsPath;
    private final String countryBlacklistPath;

    private transient Set<String> riskyMerchants;
    private transient Set<String> blacklistedCountries;

    public RiskScorer(String riskyMerchantsPath, String countryBlacklistPath) {
        this.riskyMerchantsPath = riskyMerchantsPath;
        this.countryBlacklistPath = countryBlacklistPath;
    }

    @Override
    public void open(Configuration parameters) {
        riskyMerchants = ReferenceData.loadSet(riskyMerchantsPath);
        blacklistedCountries = ReferenceData.loadSet(countryBlacklistPath);
    }

    @Override
    public Alert map(Alert alert) {
        int score = baseScore(alert.rule);

        if (alert.tx != null) {
            if (riskyMerchants.contains(alert.tx.merchantId)) {
                score += 20;
            }
            if (blacklistedCountries.contains(alert.tx.country)) {
                score += 25;
            }
            if (alert.tx.amount > 1000) {
                score += 10;
            }
        }

        alert.riskScore = Math.min(100, score);
        alert.riskLevel = level(alert.riskScore);
        return alert;
    }

    private static int baseScore(String rule) {
        switch (rule) {
            case "Impossible Travel": return 60;
            case "High Amount":       return 50;
            case "High Frequency":    return 40;
            case "Merchant Burst":    return 30;
            default:                  return 20;
        }
    }

    private static String level(int score) {
        if (score >= 70) {
            return "HIGH";
        }
        if (score >= 40) {
            return "MEDIUM";
        }
        return "LOW";
    }
}
