package com.fraud.simulator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Loads simulator configuration with the following precedence (low to high):
 *   1. Hard-coded defaults (so the simulator always runs out of the box)
 *   2. config/simulator.properties file (if present)
 *   3. Command-line overrides in key=value form, e.g. rate.per.minute=300
 *
 * This satisfies the project requirement that the number of customers,
 * cards, merchants, transaction rate and anomaly percentage are all
 * configurable via a config file or run parameters.
 */
public class SimulatorConfig {

    public int port = 9999;
    public int numCustomers = 20;
    public int numCards = 40;
    public int numMerchants = 30;
    public int ratePerMinute = 120;
    public double anomalyPercent = 2.0;
    public double latePercent = 1.0;
    public long randomSeed = -1; // -1 means non-deterministic

    public static SimulatorConfig load(String[] args) {
        SimulatorConfig cfg = new SimulatorConfig();
        Properties props = new Properties();

        // Layer 2: properties file. First CLI arg may point to a custom path.
        String path = "config/simulator.properties";
        for (String arg : args) {
            if (arg.endsWith(".properties")) {
                path = arg;
            }
        }
        try (FileInputStream in = new FileInputStream(path)) {
            props.load(in);
            System.out.println("[Simulator] Loaded configuration from " + path);
        } catch (IOException e) {
            System.out.println("[Simulator] No config file found at " + path
                    + " - using defaults / CLI overrides");
        }

        // Layer 3: key=value command-line overrides
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (eq > 0) {
                props.setProperty(arg.substring(0, eq), arg.substring(eq + 1));
            }
        }

        cfg.port = intOf(props, "simulator.port", cfg.port);
        cfg.numCustomers = intOf(props, "num.customers", cfg.numCustomers);
        cfg.numCards = intOf(props, "num.cards", cfg.numCards);
        cfg.numMerchants = intOf(props, "num.merchants", cfg.numMerchants);
        cfg.ratePerMinute = intOf(props, "rate.per.minute", cfg.ratePerMinute);
        cfg.anomalyPercent = doubleOf(props, "anomaly.percent", cfg.anomalyPercent);
        cfg.latePercent = doubleOf(props, "late.percent", cfg.latePercent);
        cfg.randomSeed = longOf(props, "random.seed", cfg.randomSeed);

        return cfg;
    }

    private static int intOf(Properties p, String key, int def) {
        return p.containsKey(key) ? Integer.parseInt(p.getProperty(key).trim()) : def;
    }

    private static double doubleOf(Properties p, String key, double def) {
        return p.containsKey(key) ? Double.parseDouble(p.getProperty(key).trim()) : def;
    }

    private static long longOf(Properties p, String key, long def) {
        return p.containsKey(key) ? Long.parseLong(p.getProperty(key).trim()) : def;
    }

    @Override
    public String toString() {
        return String.format(
                "port=%d, customers=%d, cards=%d, merchants=%d, rate=%d tx/min, anomalies=%.1f%%, late=%.1f%%",
                port, numCustomers, numCards, numMerchants, ratePerMinute, anomalyPercent, latePercent);
    }
}
