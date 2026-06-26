package com.fraud.simulator;

import com.fraud.model.Transaction;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Random;

/**
 * Entry point of the simulation layer.
 *
 * Opens a TCP server socket and streams JSON transactions, one per line,
 * to any connected client (the Flink job uses this socket as its Source).
 *
 * Real-time pacing:
 * - Inter-arrival times are drawn from an exponential distribution around
 *   the configured mean rate. This produces a natural, continuous spread
 *   of events over time (a Poisson process - the standard model for
 *   arrival streams), satisfying the requirement that events are NOT
 *   emitted in a single burst.
 * - Anomaly scenarios that consist of several transactions (e.g. a
 *   high-frequency burst) are emitted with short intra-batch delays so
 *   they land inside the relevant detection windows.
 *
 * Robustness: if the Flink job disconnects, the simulator waits for a new
 * connection instead of crashing, which makes live demos painless.
 */
public class TransactionSimulator {

    public static void main(String[] args) throws Exception {
        SimulatorConfig config = SimulatorConfig.load(args);
        System.out.println("[Simulator] Starting with: " + config);

        TransactionGenerator generator = new TransactionGenerator(config);
        Random pacing = new Random();

        // Mean gap between batches, derived from the configured rate.
        long meanIntervalMs = Math.max(1, 60_000L / config.ratePerMinute);

        try (ServerSocket server = new ServerSocket(config.port)) {
            // Outer loop: survive client disconnects and accept new clients.
            while (true) {
                System.out.println("[Simulator] Waiting for a client on port "
                        + config.port + " ...");
                try (Socket client = server.accept();
                     PrintWriter out = new PrintWriter(
                             client.getOutputStream(), true)) {

                    System.out.println("[Simulator] Client connected: "
                            + client.getRemoteSocketAddress());
                    streamTransactions(generator, out, pacing, meanIntervalMs, config);

                } catch (IOException e) {
                    System.out.println("[Simulator] Client disconnected ("
                            + e.getMessage() + ") - waiting for reconnection");
                }
            }
        }
    }

    /** Streams transactions forever (until the client disconnects). */
    private static void streamTransactions(TransactionGenerator generator,
                                           PrintWriter out,
                                           Random pacing,
                                           long meanIntervalMs,
                                           SimulatorConfig config)
            throws IOException, InterruptedException {

        long sent = 0;
        while (true) {
            List<Transaction> batch = generator.nextBatch();

            for (int i = 0; i < batch.size(); i++) {
                Transaction tx = batch.get(i);
                // Stamp event time at the true moment of emission.
                tx.eventTime = System.currentTimeMillis();

                // BONUS (event time): occasionally emit a LATE event whose
                // event_time lies 45-120 seconds in the past. The Flink job
                // must handle it via watermarks / allowed lateness / side
                // output instead of silently corrupting window results.
                if (pacing.nextDouble() * 100 < config.latePercent) {
                    tx.eventTime -= 45_000 + pacing.nextInt(75_000);
                    System.out.println("[Simulator] >> Emitting LATE event "
                            + tx.transactionId + " (event_time backdated)");
                }

                out.println(tx.toJson());
                if (out.checkError()) {
                    throw new IOException("client connection lost");
                }
                sent++;

                // Short gaps inside multi-transaction anomaly scenarios
                // (e.g. a card burst) so they stay within detection windows.
                if (i < batch.size() - 1) {
                    Thread.sleep(200 + pacing.nextInt(600));
                }
            }

            if (sent % 100 < batch.size()) {
                System.out.println("[Simulator] Sent " + sent + " transactions so far");
            }

            // Exponential inter-arrival time (Poisson process), capped at
            // 5x the mean to avoid rare very long silences during a demo.
            double u = pacing.nextDouble();
            long delay = (long) (-Math.log(1 - u) * meanIntervalMs);
            Thread.sleep(Math.min(delay, meanIntervalMs * 5));
        }
    }
}
