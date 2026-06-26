package com.fraud.processing;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fraud.model.Transaction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Deserializes raw JSON lines from the socket into Transaction objects.
 *
 * Implemented as a FlatMapFunction (not MapFunction) on purpose: a flatMap
 * may emit zero elements, which lets us silently DROP malformed lines
 * instead of crashing the whole streaming job. In a real-time pipeline,
 * one bad event must never take down the system.
 *
 * The Jackson ObjectMapper is configured with SNAKE_CASE so the JSON
 * field names required by the spec (transaction_id, card_id, ...) map
 * automatically onto the camelCase Java fields of the Transaction POJO.
 */
public class TransactionDeserializer implements FlatMapFunction<String, Transaction> {

    private static final long serialVersionUID = 1L;

    /**
     * Static + thread-safe: a single mapper shared per JVM. Declared static
     * so it is not dragged into Flink's serialization of this function.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public void flatMap(String line, Collector<Transaction> out) {
        if (line == null || line.trim().isEmpty()) {
            return; // ignore empty keep-alive lines
        }
        try {
            out.collect(MAPPER.readValue(line, Transaction.class));
        } catch (Exception e) {
            // Resilience over strictness: log and skip the bad event.
            System.err.println("[Job] Skipping malformed event: " + line);
        }
    }
}
