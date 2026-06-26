package com.fraud.processing;

import com.fraud.model.Alert;
import com.fraud.model.Transaction;
import com.fraud.processing.metrics.CategoryAverageFunction;
import com.fraud.processing.metrics.CountSumAggregate;
import com.fraud.processing.metrics.CountryCountFunction;
import com.fraud.processing.metrics.MinuteSummaryFunction;
import com.fraud.processing.risk.RiskScorer;
import com.fraud.processing.rules.HighAmountRule;
import com.fraud.processing.rules.HighFrequencyRule;
import com.fraud.processing.rules.ImpossibleTravelRule;
import com.fraud.processing.rules.MerchantBurstRule;
import com.fraud.sink.AlertSink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Main entry point of the processing layer (Parts B-E of the project).
 *
 * Pipeline overview:
 *   socket source -> JSON deserialization -> event-time watermarks
 *     -> Metric 1: global 1-minute window  (count + total amount)
 *                  + late-event handling via side output (BONUS)
 *     -> Metric 2: keyBy(category), 5-min window (average amount)
 *     -> Metric 3: keyBy(country),  5-min window (transaction count)
 *     -> Rule 1:   keyBy(card), 2-min window     (High Frequency)
 *     -> Rule 2:   keyBy(customer), keyed state  (High Amount)
 *     -> Rule 3:   keyBy(card), keyed state      (Impossible Travel)
 *     -> Rule 4:   keyBy(merchant), 2-min window (Merchant Burst, BONUS)
 *   All rule outputs are united into a single Alert stream, enriched
 *   with a risk score from external reference data (BONUS), and written
 *   to a dedicated alert sink (console stderr + alerts.log file).
 *
 * Design decisions worth explaining:
 * - EVENT TIME (bonus): windows are driven by the event_time embedded in
 *   each transaction, not by arrival time. Watermarks tolerate up to 5
 *   seconds of out-of-orderness; events arriving later than that are
 *   captured in a side output instead of being silently dropped, and
 *   events within an extra 10s of allowed lateness UPDATE the already
 *   fired window result.
 * - The same Transaction stream feeds all branches in parallel; Flink
 *   fans the events out automatically.
 * - All windows use incremental aggregation (AggregateFunction +
 *   ProcessWindowFunction) - constant memory per window.
 * - Parallelism is set to 1 for clean, ordered console output during
 *   classroom demos. In production this single line would be removed.
 * - The socket source is configured to retry forever, so the job and
 *   the simulator can be started in any order.
 */
public class FraudDetectionJob {

    /** Window sizes mandated by the project specification. */
    private static final Time MINUTE_WINDOW = Time.minutes(1);
    private static final Time TWO_MINUTE_WINDOW = Time.minutes(2);
    private static final Time FIVE_MINUTE_WINDOW = Time.minutes(5);

    /** Event-time settings (bonus): tolerated disorder + extra lateness. */
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    private static final Time ALLOWED_LATENESS = Time.seconds(10);

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.load(args);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // Single-threaded output keeps the demo console readable and ordered.
        env.setParallelism(1);
        // Restart strategy: survive transient socket disconnects (e.g. the
        // simulator is restarted mid-demo). Retries 3 times, 5 seconds apart.
        env.setRestartStrategy(
                org.apache.flink.api.common.restartstrategy.RestartStrategies
                        .fixedDelayRestart(3, 5000));

        // ----- Source: the simulator's socket, one JSON line per event -----
        // maxRetry = -1 means "retry forever": the job patiently waits if
        // the simulator is not up yet or restarts mid-demo.
        DataStream<String> rawLines =
                env.socketTextStream(config.host, config.port, "\n", -1);

        // ----- Deserialize + assign EVENT-TIME timestamps and watermarks ---
        // The watermark generator assumes events may arrive up to 5 seconds
        // out of order; beyond that they are considered "late".
        DataStream<Transaction> transactions = rawLines
                .flatMap(new TransactionDeserializer())
                .name("json-to-transaction")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Transaction>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<Transaction>)
                                                (tx, previousTimestamp) -> tx.eventTime));

        // ================= Part B: streaming metrics =================

        // Side output channel for events that arrive too late even for the
        // allowed-lateness grace period (bonus: late-event handling).
        final OutputTag<Transaction> lateEvents =
                new OutputTag<Transaction>("late-transactions") {};

        // ----- Metric 1: totals per minute (global, non-keyed) -----
        SingleOutputStreamOperator<String> minuteTotals = transactions
                .windowAll(TumblingEventTimeWindows.of(MINUTE_WINDOW))
                .allowedLateness(ALLOWED_LATENESS)
                .sideOutputLateData(lateEvents)
                .aggregate(new CountSumAggregate(), new MinuteSummaryFunction())
                .name("metric-1-minute-totals");
        minuteTotals.print();

        // Late events are NOT silently dropped - they are reported on a
        // dedicated channel (and could be routed to any sink).
        minuteTotals.getSideOutput(lateEvents)
                .printToErr("LATE EVENT (side output)");

        // ----- Metric 2: average amount per merchant category (5 min) -----
        transactions
                .keyBy(new CategoryKeySelector())
                .window(TumblingEventTimeWindows.of(FIVE_MINUTE_WINDOW))
                .aggregate(new CountSumAggregate(), new CategoryAverageFunction())
                .name("metric-2-category-average")
                .print();

        // ----- Metric 3: transaction count per country (5 min) -----
        transactions
                .keyBy(new CountryKeySelector())
                .window(TumblingEventTimeWindows.of(FIVE_MINUTE_WINDOW))
                .aggregate(new CountSumAggregate(), new CountryCountFunction())
                .name("metric-3-country-count")
                .print();

        // ================= Part C: anomaly detection =================

        // Rule 1 - High Frequency: a pure WINDOW problem.
        // keyBy(card) + 2-minute tumbling window; alert if count > 5.
        DataStream<Alert> highFrequencyAlerts = transactions
                .keyBy(new CardKeySelector())
                .window(TumblingEventTimeWindows.of(TWO_MINUTE_WINDOW))
                .aggregate(new HighFrequencyRule.FreqAggregate(),
                           new HighFrequencyRule.FreqAlertFunction())
                .name("rule-1-high-frequency");

        // Rule 2 - High Amount: a STATE problem (rolling per-customer
        // history), checked in real time on every event.
        DataStream<Alert> highAmountAlerts = transactions
                .keyBy(new CustomerKeySelector())
                .process(new HighAmountRule())
                .name("rule-2-high-amount");

        // Rule 3 - Impossible Travel: a STATE problem (last country per
        // card), also checked in real time.
        DataStream<Alert> impossibleTravelAlerts = transactions
                .keyBy(new CardKeySelector())
                .process(new ImpossibleTravelRule())
                .name("rule-3-impossible-travel");

        // Rule 4 (BONUS) - Merchant Burst: window problem keyed by merchant.
        DataStream<Alert> merchantBurstAlerts = transactions
                .keyBy(new MerchantKeySelector())
                .window(TumblingEventTimeWindows.of(TWO_MINUTE_WINDOW))
                .aggregate(new MerchantBurstRule.BurstAggregate(),
                           new MerchantBurstRule.BurstAlertFunction())
                .name("rule-4-merchant-burst");

        // Unified alert stream - all rules feed one stream of Alert objects.
        DataStream<Alert> alerts = highFrequencyAlerts
                .union(highAmountAlerts, impossibleTravelAlerts, merchantBurstAlerts);

        // ============ Part E (bonus): risk scoring + reference data ========
        // Every alert is enriched with a 0-100 risk score and a LOW /
        // MEDIUM / HIGH level, using external reference files (risky
        // merchants, country blacklist) loaded once at operator startup.
        DataStream<Alert> scoredAlerts = alerts
                .map(new RiskScorer(config.riskyMerchantsFile, config.blacklistFile))
                .name("risk-scorer");

        // ================= Part D: dedicated alert output =================
        // The alert stream gets its OWN sink, separate from the statistics:
        // stderr (renders red in IDEs) + a persistent alerts.log file.
        scoredAlerts.addSink(new AlertSink(config.alertFile))
                .name("alert-sink");

        env.execute("Real-Time Credit Card Fraud Detection");
    }

    /**
     * Key selectors are written as named classes (not lambdas) so Flink's
     * type extraction always works reliably, with no generics erasure issues.
     */
    public static class CategoryKeySelector implements KeySelector<Transaction, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(Transaction tx) {
            return tx.merchantCategory;
        }
    }

    public static class CountryKeySelector implements KeySelector<Transaction, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(Transaction tx) {
            return tx.country;
        }
    }

    public static class CardKeySelector implements KeySelector<Transaction, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(Transaction tx) {
            return tx.cardId;
        }
    }

    public static class CustomerKeySelector implements KeySelector<Transaction, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(Transaction tx) {
            return tx.customerId;
        }
    }

    public static class MerchantKeySelector implements KeySelector<Transaction, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(Transaction tx) {
            return tx.merchantId;
        }
    }
}
