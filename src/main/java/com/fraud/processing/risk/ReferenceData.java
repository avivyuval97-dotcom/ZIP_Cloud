package com.fraud.processing.risk;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Loader for external reference data files (BONUS - Part E).
 *
 * Reference files are plain text, one value per line; blank lines and
 * lines starting with '#' are ignored. The project ships two such files:
 *   config/risky_merchants.txt   - merchants flagged as high-risk
 *   config/country_blacklist.txt - high-risk countries
 *
 * Loading is fail-soft: a missing file logs a warning and yields an
 * empty set, so the job never crashes because of optional enrichment
 * data - a deliberate robustness decision.
 */
public final class ReferenceData {

    private ReferenceData() {
    }

    public static Set<String> loadSet(String path) {
        Set<String> values = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                    values.add(trimmed);
                }
            }
            System.out.println("[ReferenceData] Loaded " + values.size()
                    + " entries from " + path);
        } catch (Exception e) {
            System.err.println("[ReferenceData] Could not read " + path
                    + " (" + e.getMessage() + ") - continuing with empty set");
        }
        return values;
    }
}
