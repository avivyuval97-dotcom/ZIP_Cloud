# Project Guide — Real-Time Credit Card Fraud Detection (Apache Flink)

A complete walkthrough of **what this project is** and **what every single file does**.
Use this as the "explain-the-whole-thing" document (e.g. to paste into the Claude
app, or to onboard a reviewer). For run instructions and the assignment write-up,
see `README.md`.

---

## 1. What the project is

A **streaming fraud-detection pipeline** built for the course
*"Introduction to Cloud Data Services."* It has two halves that talk over a
plain TCP socket:

1. **A transaction simulator** (dependency-free Java) that pretends to be a
   payment network. It invents a realistic world of customers, cards and
   merchants and streams **one JSON transaction per line** over TCP port 9999.
   It also deliberately injects a configurable percentage of *anomalies* and
   *late events* so the detector has something to catch.

2. **An Apache Flink job** that connects to that socket as its data source and,
   in real time:
   - computes **streaming metrics** (per-minute totals, 5-min averages/counts),
   - runs **four anomaly-detection rules** in parallel,
   - **risk-scores** each alert using external reference data,
   - and writes alerts to a **dedicated output** (stderr + `output/alerts.log`),
     kept separate from the metrics output.

### The data flow at a glance

```
TransactionSimulator (TCP :9999)
        │  one JSON line per transaction
        ▼
FraudDetectionJob (Apache Flink)
        │  parse JSON ─► assign event-time + watermarks
        ├── Metric 1: 1-min global totals            (+ late-event side output)
        ├── Metric 2: 5-min avg amount per category
        ├── Metric 3: 5-min tx count per country
        ├── Rule 1:  High Frequency   (window, keyBy card)
        ├── Rule 2:  High Amount      (state,  keyBy customer)
        ├── Rule 3:  Impossible Travel(state,  keyBy card)
        └── Rule 4:  Merchant Burst   (window, keyBy merchant)   ← bonus
                 │  all rule outputs union into one Alert stream
                 ▼
        RiskScorer  (adds 0-100 score using config/*.txt reference data)
                 ▼
        AlertSink   ─► stderr (red)  +  output/alerts.log
```

### The four detection rules

| Rule | Trigger | Flink technique | Why that technique |
|---|---|---|---|
| **High Frequency** | > 5 tx on one **card** in 2 min | `keyBy(card)` + 2-min tumbling window + incremental aggregate | counting events in a fixed time slice = a window |
| **High Amount** | tx ≥ 3× avg of customer's last 20 tx **and** > 500 | `keyBy(customer)` + `ListState` | compares to rolling *history*, not a time slice |
| **Impossible Travel** | same **card**, 2 countries < 30 min apart | `keyBy(card)` + `ValueState` | only needs to "remember the last thing" |
| **Merchant Burst** (bonus) | > 20 tx on one **merchant** in 2 min | `keyBy(merchant)` + 2-min tumbling window | card-testing attack = a window count |

### Bonus features implemented
- **Event time + watermarks + late events** — windows run on each event's
  embedded `event_time`; events up to 5s out of order are tolerated, an extra
  10s of *allowed lateness* updates already-fired windows, and anything later
  goes to a **side output** instead of being dropped.
- **A 4th rule** (Merchant Burst).
- **Risk scoring** — an explainable 0-100 additive score with LOW/MEDIUM/HIGH.
- **External reference data** — risky merchants and blacklisted countries loaded
  from text files at operator startup.

---

## 2. Directory layout

```
ZIP_Cloud/
├── README.md                 Assignment write-up + run instructions
├── PROJECT_GUIDE.md          ← this file (full project + per-file explanation)
├── pom.xml                   Maven build: dependencies + fat-jar packaging
├── flink-fraud-detection.iml IntelliJ module file (IDE metadata)
├── config/                   All runtime configuration & reference data
│   ├── simulator.properties
│   ├── risky_merchants.txt
│   └── country_blacklist.txt
├── output/                   Generated at runtime (alerts.log is written here)
├── .claude/
│   └── settings.local.json   Claude Code permission settings for this project
└── src/main/java/com/fraud/
    ├── simulator/            Simulation layer (zero external dependencies)
    │   ├── SimulatorConfig.java
    │   ├── TransactionGenerator.java
    │   └── TransactionSimulator.java
    ├── model/                Shared data model (POJOs)
    │   ├── Transaction.java
    │   └── Alert.java
    ├── processing/           Flink job: the actual stream processing
    │   ├── FraudDetectionJob.java
    │   ├── JobConfig.java
    │   ├── TransactionDeserializer.java
    │   ├── metrics/          Streaming metric operators
    │   │   ├── CountSumAccumulator.java
    │   │   ├── CountSumAggregate.java
    │   │   ├── MinuteSummaryFunction.java
    │   │   ├── CategoryAverageFunction.java
    │   │   └── CountryCountFunction.java
    │   ├── rules/            The four anomaly-detection rules
    │   │   ├── HighFrequencyRule.java
    │   │   ├── HighAmountRule.java
    │   │   ├── ImpossibleTravelRule.java
    │   │   └── MerchantBurstRule.java
    │   └── risk/             Risk scoring + reference data (bonus)
    │       ├── ReferenceData.java
    │       └── RiskScorer.java
    └── sink/                 Output layer
        └── AlertSink.java
```

The code is split into **layers** on purpose: the `simulator` layer has **zero
external dependencies** (so it compiles and runs with a plain `javac`/`java`),
while only the `processing` layer pulls in Flink.

---

## 3. Every file, explained

### Top-level

- **`README.md`** — The assignment-facing document: project description, how to
  run the simulator (Part A) and the Flink job (Part B), the rules (Part C), the
  alert stream (Part D), bonus items (Part E), calibration notes, and Java-17
  troubleshooting. Read this to *run* the project.

- **`PROJECT_GUIDE.md`** — This file. Explains the whole project plus every file.

- **`pom.xml`** — Maven build configuration. Declares three dependencies:
  `flink-streaming-java` (the DataStream API), `flink-clients` (lets the job run
  in an embedded local mini-cluster — no Flink install needed), and
  `jackson-databind` (JSON parsing). Also configures two plugins: `exec-maven-plugin`
  (for `mvn exec:java`) and `maven-shade-plugin` (builds a single runnable
  "fat jar" with `FraudDetectionJob` as the main class). Targets Java 11.

- **`flink-fraud-detection.iml`** — IntelliJ IDEA module descriptor. IDE-generated
  metadata (source roots, dependency references). Not needed to build with Maven;
  safe to ignore unless you use IntelliJ.

### `config/` — runtime configuration & reference data

- **`config/simulator.properties`** — The simulator's settings file. Controls
  port, world size (`num.customers=50`, `num.cards=200`, `num.merchants=30`),
  `rate.per.minute=120`, `anomaly.percent=2.0`, `late.percent=1.0`, and
  `random.seed=42`. The comments document the *calibration* rationale: cards are
  intentionally numerous relative to the rate (so a normal card sees ~1.2 tx per
  2-min window, well under the High-Frequency threshold), and the **fixed seed**
  keeps the simulated world identical across restarts so the Flink job's keyed
  state never goes stale mid-demo.

- **`config/risky_merchants.txt`** — External reference data (bonus). One
  high-risk merchant id per line (`m-7`, `m-13`, `m-23`); `#` lines are comments.
  Loaded by the `RiskScorer`; a matching merchant adds **+20** to an alert's risk.

- **`config/country_blacklist.txt`** — External reference data (bonus). One
  high-risk country code per line (`NL`, `ES`). Loaded by the `RiskScorer`; a
  matching country adds **+25** to an alert's risk. The values are chosen from the
  simulator's country pool so the enrichment is actually observable in alerts.

### `output/` — generated at runtime

- **`output/alerts.log`** — *(generated; currently empty/absent)* The persistent
  alert log. The `AlertSink` appends every detected alert here. Configurable via
  `alert.file=...`. This is generated output, not source.

### `.claude/`

- **`.claude/settings.local.json`** — Claude Code project settings. Pre-approves a
  few safe commands (`java *`, `Stop-Process *`, `Get-Content *`, and the IDE
  diagnostics tool) so they don't prompt for permission. Tooling config only — not
  part of the application.

---

### `src/main/java/com/fraud/simulator/` — Simulation layer (Part A)

> Dependency-free on purpose, so it can run standalone with plain `java`.

- **`SimulatorConfig.java`** — Loads simulator configuration with three-level
  precedence: hard-coded defaults → `config/simulator.properties` → `key=value`
  command-line overrides. Exposes fields like `numCustomers`, `numCards`,
  `ratePerMinute`, `anomalyPercent`, `latePercent`, `randomSeed`. This is what
  makes the simulated world fully configurable as the spec requires.

- **`TransactionGenerator.java`** — The "brain" of the simulator. At startup it
  builds a **static world**: each merchant is permanently tied to a category, each
  customer is tagged *standard* (cheap categories, amounts ≤ 250) or *premium*
  (electronics/travel, ~600–1600), and each card is assigned to a customer plus a
  fixed **home country**. `nextBatch()` normally returns one realistic
  transaction (amounts skewed cheap via `rand²`), but with probability
  `anomaly.percent` it instead emits a deliberate **anomaly scenario**:
  `highAmountScenario` (6×–10× spike), `highFrequencyScenario` (6–8 rapid tx on
  one card), `impossibleTravelScenario` (same card, two countries), or
  `merchantBurstScenario` (15–20 tx hammering one merchant). The spending-profile
  and home-country design *mathematically guarantees* normal traffic can't trip
  the rules — only injected anomalies do.

- **`TransactionSimulator.java`** — The entry point of the simulator (`main`).
  Opens a TCP `ServerSocket`, waits for a client (the Flink job), then streams
  JSON transactions one per line. Pacing is realistic: inter-arrival gaps follow
  an **exponential distribution** (a Poisson process), so events spread naturally
  over time instead of bursting. It stamps `event_time` at the moment of emission,
  occasionally **backdates** an event by 45–120s (the bonus "late event" path),
  and survives client disconnects by looping back to `accept()` a new connection.

### `src/main/java/com/fraud/model/` — Shared data model

- **`Transaction.java`** — POJO for one transaction (`transactionId`, `cardId`,
  `customerId`, `merchantId`, `merchantCategory`, `country`, `eventTime`,
  `amount`). It's a valid **Flink POJO** (public fields + no-arg constructor) so
  Flink serializes it efficiently. `toJson()` builds the snake_case JSON line
  *manually* (no library) to keep the simulator dependency-free; `Locale.US`
  forces a dot decimal separator. Shared by both layers = one source of truth for
  the event schema.

- **`Alert.java`** — POJO for a detected anomaly. Carries the `timestamp`, the
  entity (`entityType` = Card/Customer/Merchant + `entityId`), the `rule` that
  fired, rule-specific `decisionValues`, the full suspicious `Transaction`, and
  the bonus `riskScore`/`riskLevel` (filled in later by the `RiskScorer`).
  `toString()` formats the canonical "Anomaly Detected! ..." line you see in the
  log. Also a valid Flink POJO so alert streams flow between operators.

### `src/main/java/com/fraud/processing/` — The Flink job

- **`FraudDetectionJob.java`** — **The main entry point and orchestrator.** Sets
  up the `StreamExecutionEnvironment`, reads the socket source (retries forever so
  start order doesn't matter), deserializes JSON, and assigns **event-time
  watermarks** (5s out-of-orderness). It then wires up the whole pipeline: the
  three metrics (with the late-event side output on Metric 1), the four rules,
  `union`s all rule outputs into one Alert stream, pipes it through the
  `RiskScorer`, and attaches the `AlertSink`. Parallelism is set to 1 for clean,
  ordered demo output. Also defines the named `KeySelector` classes
  (card/customer/country/category/merchant) used throughout.

- **`JobConfig.java`** — Minimal config for the Flink job. Defaults plus
  `key=value` CLI overrides for `socket.host`, `socket.port`, `alert.file`,
  `risk.merchants.file`, `risk.blacklist.file`. Mirrors the simulator's config style.

- **`TransactionDeserializer.java`** — A `FlatMapFunction<String, Transaction>`
  that parses each raw JSON line into a `Transaction` using a shared Jackson
  `ObjectMapper` configured for **SNAKE_CASE**. It's a flatMap (not map) on
  purpose: a malformed line emits **zero elements** (logged and skipped) instead
  of crashing the streaming job — resilience over strictness.

#### `processing/metrics/` — the three streaming metrics (Part B)

- **`CountSumAccumulator.java`** — A tiny accumulator holding just a running
  `count` and `sum`. Two numbers per window (not the full event buffer) is what
  keeps window aggregation memory-safe on an unbounded stream. Powers all three
  metrics.

- **`CountSumAggregate.java`** — The `AggregateFunction` that incrementally
  updates a `CountSumAccumulator` as each event arrives (O(1) state per window,
  result ready the instant the window closes). Includes the required `merge()`.

- **`MinuteSummaryFunction.java`** — Metric 1. A `ProcessAllWindowFunction`
  (global/non-keyed) that takes the pre-aggregated count+sum for each 1-minute
  window and formats: `Timestamp: …, Total Transactions: …, Total Amount: …`.

- **`CategoryAverageFunction.java`** — Metric 2. A `ProcessWindowFunction` keyed
  by `merchant_category` over 5-min windows; computes `sum/count` and formats:
  `Timestamp: …, Category: …, Average Amount: …`.

- **`CountryCountFunction.java`** — Metric 3. A `ProcessWindowFunction` keyed by
  `country` over 5-min windows; uses only the count and formats:
  `Timestamp: …, Country: …, Transactions: …`.

#### `processing/rules/` — the four detection rules (Parts C + bonus)

- **`HighFrequencyRule.java`** — Rule 1 (window). Inner classes: `FreqAccumulator`
  (count + latest tx), `FreqAggregate` (incremental count per card/window), and
  `FreqAlertFunction` (emits an Alert when count > 5 at window close). Keyed by card.

- **`HighAmountRule.java`** — Rule 2 (state). A `KeyedProcessFunction` keyed by
  customer using **`ListState`** of the last 20 amounts. On each event it checks
  *before* appending (so the spike can't dilute its own baseline), requires a
  minimum history (cold-start guard), and alerts when `amount ≥ 3 × avg` **and**
  `> 500`.

- **`ImpossibleTravelRule.java`** — Rule 3 (state). A `KeyedProcessFunction` keyed
  by card using a single **`ValueState`** (`LastLocation` = country + time). Alerts
  when the new country differs from the last one and the gap is < 30 minutes, then
  updates the stored location. The minimal-state counterpart to Rule 2's ListState.

- **`MerchantBurstRule.java`** — Rule 4 (bonus, window). Mirror of Rule 1 but keyed
  by merchant with a threshold of > 20 tx per 2-min window (organic load ~9 → only
  injected card-testing bursts trigger it). Inner classes `BurstAccumulator`,
  `BurstAggregate`, `BurstAlertFunction`. The doc comment notes the tumbling-window
  boundary trade-off honestly.

#### `processing/risk/` — risk scoring + reference data (bonus, Part E)

- **`ReferenceData.java`** — Utility loader for the plain-text reference files
  (one value per line, `#` = comment). **Fail-soft**: a missing file logs a warning
  and returns an empty set rather than crashing the job.

- **`RiskScorer.java`** — A `RichMapFunction<Alert, Alert>` that scores every alert.
  Loads the two reference files **once** in `open()`. Additive, explainable model:
  base by rule severity (Impossible Travel 60, High Amount 50, High Frequency 40,
  Merchant Burst 30) **+20** risky merchant **+25** blacklisted country **+10**
  amount > 1000, capped at 100, then mapped to LOW (<40) / MEDIUM (40–69) /
  HIGH (≥70).

### `src/main/java/com/fraud/sink/` — Output layer (Part D)

- **`AlertSink.java`** — A `RichSinkFunction<Alert>` giving the alert stream its
  **own** output, separate from the metrics. Opens the log file once in `open()`
  (creating the `output/` dir if needed, append mode), and on each alert writes to
  **stderr** (renders red in IDEs, so alerts pop out of the metric stream) **and**
  appends to `output/alerts.log`, flushing immediately so alerts never sit in a
  buffer. Closes the file handle cleanly on shutdown.

---

## 4. How to run (quick reference)

See `README.md` for full detail. The short version:

1. **Start the simulator** (it waits for a client on port 9999):
   ```
   mvn compile exec:java -Dexec.mainClass=com.fraud.simulator.TransactionSimulator
   ```
2. **Start the Flink job** (connects to the simulator; embedded mini-cluster):
   ```
   mvn compile exec:java -Dexec.mainClass=com.fraud.processing.FraudDetectionJob
   ```
3. Watch **metrics on stdout** and **alerts on stderr / `output/alerts.log`**.
   Metric 1 appears after the first full minute; Metrics 2–3 after 5 minutes.
```
