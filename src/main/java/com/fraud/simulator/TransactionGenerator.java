package com.fraud.simulator;

import com.fraud.model.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Generates realistic credit-card transactions, including a configurable
 * percentage of intentionally anomalous ones.
 *
 * Realism + calibration choices (deliberate, see project docs):
 * - Each merchant is permanently associated with one merchant category,
 *   and each category has its own typical amount range, as the spec requires.
 * - Amounts are skewed toward the low end of each range (rand^2),
 *   mimicking real spending: many small purchases, few large ones.
 * - SPENDING PROFILES: every customer is either "standard" (shops in cheap
 *   categories: groceries, fuel, restaurants) or "premium" (electronics,
 *   travel). This keeps each customer's historical average representative
 *   of their own behavior, which mathematically guarantees that NORMAL
 *   traffic can never trigger the High Amount rule:
 *     * standard customers: every normal amount <= 250 < 500 (rule's floor)
 *     * premium customers:  amounts within [600, 1600]; max/min = 2.67 < 3x
 *   Only deliberately injected anomalies (6x-10x) cross the thresholds.
 * - Each card belongs to exactly one customer and has a fixed "home
 *   country" where all of its legitimate transactions occur, so the
 *   Impossible Travel rule fires only on deliberately injected anomalies.
 *
 * Anomaly injection: with probability anomalyPercent, instead of a single
 * normal transaction the generator emits a small "scenario" (batch):
 *   HIGH_AMOUNT       - one transaction far above the customer's normal range
 *   HIGH_FREQUENCY    - 6-8 rapid transactions on the same card
 *   IMPOSSIBLE_TRAVEL - two transactions on the same card in two countries
 * The batch structure lets the main loop control real-time pacing.
 */
public class TransactionGenerator {

    /**
     * Categories required by the spec, each with [min, max] amount range.
     * Split into two spending tiers (see class comment for why).
     */
    private static final String[] ALL_CATEGORIES =
            {"groceries", "fuel", "restaurants", "electronics", "travel"};
    private static final Set<String> CHEAP_CATEGORIES = new HashSet<>(
            List.of("groceries", "fuel", "restaurants"));

    private static final Map<String, double[]> AMOUNT_RANGES = new HashMap<>();
    static {
        AMOUNT_RANGES.put("groceries",   new double[]{10, 150});
        AMOUNT_RANGES.put("fuel",        new double[]{20, 120});
        AMOUNT_RANGES.put("restaurants", new double[]{15, 250});
        AMOUNT_RANGES.put("electronics", new double[]{600, 1400});
        AMOUNT_RANGES.put("travel",      new double[]{700, 1600});
    }

    /** Share of customers with a "premium" (expensive) spending profile. */
    private static final double PREMIUM_CUSTOMER_RATIO = 0.35;

    private static final String[] COUNTRIES =
            {"IL", "US", "FR", "DE", "UK", "IT", "ES", "NL"};

    private final SimulatorConfig config;
    private final Random random;
    private long txCounter = 0;

    /** Static world, built once at startup. */
    private final Map<String, String> cardToCustomer = new HashMap<>();
    private final Map<String, String> cardToHomeCountry = new HashMap<>();
    private final List<String> cardIds = new ArrayList<>();
    private final Set<String> premiumCustomers = new HashSet<>();
    private final Map<String, String> merchantCategory = new HashMap<>();
    private final List<String> cheapMerchants = new ArrayList<>();
    private final List<String> expensiveMerchants = new ArrayList<>();

    public TransactionGenerator(SimulatorConfig config) {
        this.config = config;
        this.random = (config.randomSeed >= 0)
                ? new Random(config.randomSeed) : new Random();

        // Merchants: index deterministically maps to a category, so the
        // same merchant always sells in the same category (realistic).
        for (int i = 0; i < config.numMerchants; i++) {
            String merchantId = "m-" + i;
            String category = ALL_CATEGORIES[i % ALL_CATEGORIES.length];
            merchantCategory.put(merchantId, category);
            if (CHEAP_CATEGORIES.contains(category)) {
                cheapMerchants.add(merchantId);
            } else {
                expensiveMerchants.add(merchantId);
            }
        }
        // Safety for tiny configurations: never leave a tier empty.
        if (cheapMerchants.isEmpty()) {
            cheapMerchants.addAll(merchantCategory.keySet());
        }
        if (expensiveMerchants.isEmpty()) {
            expensiveMerchants.addAll(merchantCategory.keySet());
        }

        // Customers: assign a spending profile to each.
        for (int i = 0; i < config.numCustomers; i++) {
            if (random.nextDouble() < PREMIUM_CUSTOMER_RATIO) {
                premiumCustomers.add("cust-" + i);
            }
        }

        // Cards: distributed across customers round-robin so every
        // customer owns at least one card; each card has a home country.
        for (int i = 0; i < config.numCards; i++) {
            String cardId = "card-" + i;
            cardIds.add(cardId);
            cardToCustomer.put(cardId, "cust-" + (i % config.numCustomers));
            cardToHomeCountry.put(cardId,
                    COUNTRIES[random.nextInt(COUNTRIES.length)]);
        }
    }

    /**
     * Returns the next batch of transactions to emit.
     * Usually a single normal transaction; occasionally an anomaly scenario.
     * eventTime is left as 0 and stamped by the sender at emission time,
     * so event timestamps always reflect true real-time pacing.
     */
    public List<Transaction> nextBatch() {
        if (random.nextDouble() * 100 < config.anomalyPercent) {
            int type = random.nextInt(4);
            switch (type) {
                case 0:  return highAmountScenario();
                case 1:  return highFrequencyScenario();
                case 2:  return impossibleTravelScenario();
                default: return merchantBurstScenario();
            }
        }
        List<Transaction> batch = new ArrayList<>(1);
        batch.add(normalTransaction(randomCard(), null, null));
        return batch;
    }

    // ----- normal traffic -------------------------------------------------

    private Transaction normalTransaction(String cardId, String forcedCountry,
                                          String forcedMerchant) {
        String customerId = cardToCustomer.get(cardId);

        // The customer's spending profile decides WHERE they shop
        // (unless a specific merchant is forced by an anomaly scenario).
        String merchantId = forcedMerchant;
        if (merchantId == null) {
            List<String> merchants = premiumCustomers.contains(customerId)
                    ? expensiveMerchants : cheapMerchants;
            merchantId = merchants.get(random.nextInt(merchants.size()));
        }
        String category = merchantCategory.get(merchantId);
        double[] range = AMOUNT_RANGES.get(category);

        // rand^2 skews amounts toward the cheap end of the range.
        double skewed = Math.pow(random.nextDouble(), 2);
        double amount = round2(range[0] + skewed * (range[1] - range[0]));

        // Legitimate transactions always occur in the card's home country.
        // Cross-country activity is produced ONLY by deliberate anomaly
        // injection, so observed anomalies match anomaly.percent exactly.
        String country = (forcedCountry != null) ? forcedCountry
                : cardToHomeCountry.get(cardId);

        return new Transaction(
                "tx-" + (100000 + txCounter++),
                cardId,
                customerId,
                merchantId,
                category,
                country,
                0L, // stamped at emission time
                amount);
    }

    // ----- anomaly scenarios ----------------------------------------------

    /** One transaction 6x-10x a typical amount, always well above 500. */
    private List<Transaction> highAmountScenario() {
        Transaction tx = normalTransaction(randomCard(), null, null);
        double factor = 6 + random.nextDouble() * 4;
        // Floor is randomized (600-1000) so injected amounts never look
        // suspiciously identical for low-spending customers.
        double floor = 600 + random.nextDouble() * 400;
        tx.amount = round2(Math.max(floor, tx.amount * factor));
        log("HIGH_AMOUNT", tx.cardId, String.format(
                java.util.Locale.US, "amount=%.2f", tx.amount));
        List<Transaction> batch = new ArrayList<>(1);
        batch.add(tx);
        return batch;
    }

    /** 6-8 rapid transactions on the same card (rule: >5 in 2 minutes). */
    private List<Transaction> highFrequencyScenario() {
        String cardId = randomCard();
        int count = 6 + random.nextInt(3);
        List<Transaction> batch = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            batch.add(normalTransaction(cardId, null, null));
        }
        log("HIGH_FREQUENCY", cardId, count + " rapid transactions");
        return batch;
    }

    /** Two transactions on one card in two different countries. */
    private List<Transaction> impossibleTravelScenario() {
        String cardId = randomCard();
        String home = cardToHomeCountry.get(cardId);
        String abroad = randomOtherCountry(home);
        List<Transaction> batch = new ArrayList<>(2);
        batch.add(normalTransaction(cardId, home, null));
        batch.add(normalTransaction(cardId, abroad, null));
        log("IMPOSSIBLE_TRAVEL", cardId, home + " -> " + abroad);
        return batch;
    }

    /**
     * BONUS scenario: 15-20 rapid transactions from many different cards
     * hitting ONE merchant (card-testing attack pattern). The merchant is
     * chosen from the cheap tier so the small amounts (<= 250) cannot
     * accidentally trigger the High Amount rule - the scenario isolates
     * the Merchant Burst rule.
     */
    private List<Transaction> merchantBurstScenario() {
        String merchantId = cheapMerchants.get(random.nextInt(cheapMerchants.size()));
        int count = 15 + random.nextInt(6);
        List<Transaction> batch = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            batch.add(normalTransaction(randomCard(), null, merchantId));
        }
        log("MERCHANT_BURST", merchantId,
                count + " rapid transactions from many cards");
        return batch;
    }

    // ----- helpers ----------------------------------------------------------

    private String randomCard() {
        return cardIds.get(random.nextInt(cardIds.size()));
    }

    private String randomOtherCountry(String exclude) {
        String c;
        do {
            c = COUNTRIES[random.nextInt(COUNTRIES.length)];
        } while (c.equals(exclude));
        return c;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    /** Logs injected anomalies so demo runs can be validated end-to-end. */
    private void log(String type, String entity, String details) {
        System.out.printf("[Simulator] >> Injecting anomaly %s | %s | %s%n",
                type, entity, details);
    }
}
