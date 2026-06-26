package com.fraud.processing;

/**
 * Minimal configuration for the Flink job.
 *
 * Mirrors the simulator's configuration style: sensible defaults that can
 * be overridden with key=value command-line arguments, e.g.:
 *   socket.host=localhost socket.port=9999
 */
public class JobConfig {

    public String host = "localhost";
    public int port = 9999;
    public String alertFile = "output/alerts.log";
    public String riskyMerchantsFile = "config/risky_merchants.txt";
    public String blacklistFile = "config/country_blacklist.txt";

    public static JobConfig load(String[] args) {
        JobConfig cfg = new JobConfig();
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String key = arg.substring(0, eq);
            String value = arg.substring(eq + 1).trim();
            if (key.equals("socket.host")) {
                cfg.host = value;
            } else if (key.equals("socket.port")) {
                cfg.port = Integer.parseInt(value);
            } else if (key.equals("alert.file")) {
                cfg.alertFile = value;
            } else if (key.equals("risk.merchants.file")) {
                cfg.riskyMerchantsFile = value;
            } else if (key.equals("risk.blacklist.file")) {
                cfg.blacklistFile = value;
            }
        }
        return cfg;
    }
}
