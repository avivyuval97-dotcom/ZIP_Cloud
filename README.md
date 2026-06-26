# Real-Time Credit Card Fraud Detection — Apache Flink

Final project for "Introduction to Cloud Data Services".
Java + Apache Flink 1.18 (DataStream API).

## Project structure (layer separation)

```
src/main/java/com/fraud/
├── simulator/    Simulation layer  - generates the transaction stream (zero dependencies)
├── model/        Data model layer  - Transaction / Alert POJOs
├── processing/   Processing layer  - Flink job: metrics, windows, keyed state, anomaly rules
└── sink/         Output layer      - statistics stream + dedicated alert stream
config/           All configurable parameters
```

## Prerequisites

- Java 11+ (JDK)
- Maven 3.6+

## Running the simulator (Part A)

The simulator is dependency-free, so it can be compiled and run directly:

```bash
# compile (simulator + model only)
javac -d target/classes src/main/java/com/fraud/model/Transaction.java \
                        src/main/java/com/fraud/simulator/*.java

# run with the default configuration file (config/simulator.properties)
java -cp target/classes com.fraud.simulator.TransactionSimulator
```

Every parameter can be overridden on the command line:

```bash
java -cp target/classes com.fraud.simulator.TransactionSimulator \
     rate.per.minute=300 anomaly.percent=5 num.customers=50 num.cards=100
```

The simulator listens on TCP port 9999 (configurable) and streams one JSON
transaction per line to the connected client. To watch the raw stream:

```bash
nc localhost 9999
```

Sample event:

```json
{"transaction_id":"tx-100002","card_id":"card-4","customer_id":"cust-4","merchant_id":"m-16","merchant_category":"electronics","country":"DE","event_time":1781117688095,"amount":1255.49}
```

When the simulator deliberately injects an anomaly (per `anomaly.percent`),
it logs it to its own console, so demo runs can be validated end-to-end:

```
[Simulator] >> Injecting anomaly IMPOSSIBLE_TRAVEL | card=card-3 | IL -> DE
[Simulator] >> Injecting anomaly HIGH_AMOUNT | card=card-26 | amount=6255.27
[Simulator] >> Injecting anomaly HIGH_FREQUENCY | card=card-21 | 6 rapid transactions
```

## Running the Flink job (Part B)

The easiest way is from IntelliJ: open
`src/main/java/com/fraud/processing/FraudDetectionJob.java`
and click the green Run arrow next to `main`.
No Flink installation is needed - the `flink-clients` dependency runs an
embedded local mini-cluster inside the JVM.

From the command line:

```bash
mvn compile exec:java -Dexec.mainClass=com.fraud.processing.FraudDetectionJob
```

Optional socket overrides: `socket.host=localhost socket.port=9999`

Recommended demo flow:
1. Start the simulator (it waits for a client on port 9999)
2. Start the Flink job (it connects, and the simulator logs "Client connected")
3. Metric 1 prints after the first full minute; metrics 2-3 after 5 minutes

Sample output:

```
Timestamp: 1715606460000, Total Transactions: 854, Total Amount: 428930.70
Timestamp: 1715606700000, Category: electronics, Average Amount: 611.35
Timestamp: 1715606700000, Country: IL, Transactions: 497
```

## Anomaly detection (Part C) and the alert stream (Part D)

Three detection rules run in parallel with the metrics, on the same
transaction stream:

| Rule | Technique | Why |
|---|---|---|
| High Frequency (>5 tx / card / 2 min) | keyBy(card) + 2-min tumbling window | counting inside a fixed time slice is the definition of a window |
| High Amount (>=3x avg of last 20, and >500) | keyBy(customer) + ListState | compares an event to rolling HISTORY, not to a time slice |
| Impossible Travel (2 countries < 30 min) | keyBy(card) + ValueState | "remember the last thing" - minimal keyed state |

All rule outputs are united into one Alert stream which goes to a
DEDICATED sink (Part D), separate from the statistics:
- console: alerts print to stderr (red in IDEs), metrics to stdout
- file: alerts are appended to `output/alerts.log` (configurable via
  `alert.file=...`), a persistent record of every detection

Sample alert output:

```
Anomaly Detected! Timestamp: 1781122684086, Customer: cust-15, Rule: High Amount, Amount: 2244.67, Historical Avg: 412.30 | Suspicious Tx: tx-100431, merchant m-8 (electronics), country DE, amount 2244.67
Anomaly Detected! Timestamp: 1781122673890, Card: card-1, Rule: High Frequency, Transactions in 2 minutes: 7 | Suspicious Tx: tx-100398, merchant m-12 (fuel), country IL, amount 45.10
Anomaly Detected! Timestamp: 1781123341681, Card: card-138, Rule: Impossible Travel, Previous Country: FR, Current Country: NL, Time Gap Minutes: 0 | Suspicious Tx: tx-100502, merchant m-3 (groceries), country NL, amount 88.20
```

### Calibration notes (false-positive elimination)

Two deliberate calibration decisions keep alerts aligned with the
configured anomaly percentage (documented here because they were
discovered through live testing):

1. **Card density vs. High Frequency threshold**: with 200 cards at
   120 tx/min, a normal card sees ~1.2 tx per 2-minute window - far
   below the threshold of 5, so only injected bursts trigger Rule 1.
2. **Customer spending profiles vs. High Amount rule**: every customer
   is either "standard" (groceries/fuel/restaurants, amounts <= 250 -
   below the rule's 500 floor) or "premium" (electronics/travel,
   amounts in [600,1600] - max/min ratio 2.67 < 3x). Normal traffic
   therefore cannot mathematically trigger Rule 2; only 6x-10x
   injected anomalies can.
3. **Fixed random seed (42)**: the simulated world (home countries,
   profiles) is identical across simulator restarts, so the job's keyed
   state never goes stale mid-demo.

## Bonus (Part E) - all four items implemented

**1. Event time + watermarks + late events.** All windows run on the
event_time embedded in each transaction (TumblingEventTimeWindows), with
watermarks tolerating 5 seconds of out-of-orderness. Beyond that, a
10-second allowed-lateness grace period lets late events UPDATE an
already-fired window result; events later than both are captured in a
dedicated SIDE OUTPUT (printed to stderr as "LATE EVENT") instead of
being silently dropped. The simulator deliberately backdates a
configurable percentage of events (`late.percent`) by 45-120 seconds to
exercise this path.

**2. Additional rule - Merchant Burst.** More than 20 transactions for
one merchant in a 2-minute window (organic load ~9) indicates a
card-testing attack. The simulator injects matching bursts of 15-20
transactions from many cards at a single merchant.

**3. Risk score.** Every alert passes through a RiskScorer that assigns
a 0-100 score (additive, explainable model: rule severity base + risky
merchant +20 + blacklisted country +25 + amount over 1000 +10) and a
LOW / MEDIUM / HIGH level, included in the alert output.

**4. External reference data.** `config/risky_merchants.txt` and
`config/country_blacklist.txt` are loaded once at operator startup
(RichMapFunction.open) and feed the risk score. Loading is fail-soft:
missing files degrade to empty sets rather than crashing the job.

Sample scored alert:

```
Anomaly Detected! Timestamp: 1781161196980, Card: card-111, Rule: Impossible Travel, Previous Country: IL, Current Country: NL, Time Gap Minutes: 0 | Risk Score: 85/100 (HIGH) | Suspicious Tx: tx-100627, merchant m-20 (groceries), country NL, amount 128.82
```

### Troubleshooting

If the job fails on Java 17 with an error mentioning
`InaccessibleObjectException` or `module java.base does not "opens" ...`,
add these VM options to the run configuration
(Run > Edit Configurations > Modify options > Add VM options):

```
--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED
```

