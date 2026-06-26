package com.fraud.sink;

import com.fraud.model.Alert;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * Dedicated sink for the anomaly alert stream (Part D of the project).
 *
 * The spec requires the alert stream to be SEPARATE from the regular
 * statistical output. This sink delivers that separation on two levels:
 *
 * 1. Console: alerts are written to STDERR while metrics go to STDOUT.
 *    In IDEs (and in most terminals) stderr is rendered in red, so during
 *    a live demo the alerts visually pop out of the metric stream.
 *
 * 2. File: every alert is also appended to a log file (alerts.log by
 *    default), giving a persistent record that survives the run - useful
 *    as the "clear example of valid output" the spec asks to present.
 *
 * Implemented as a RichSinkFunction so the file handle is opened once per
 * task (open) and closed cleanly on shutdown (close), not per record.
 * Each alert is flushed immediately - alert delivery must not wait in a
 * buffer.
 */
public class AlertSink extends RichSinkFunction<Alert> {

    private static final long serialVersionUID = 1L;

    private final String filePath;
    private transient PrintWriter fileWriter;

    public AlertSink(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        File file = new File(filePath);
        if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }
        // append = true: consecutive demo runs accumulate into one log.
        fileWriter = new PrintWriter(new FileWriter(file, true), false);
        System.err.println("[AlertSink] Writing alerts to " + file.getAbsolutePath());
    }

    @Override
    public void invoke(Alert alert, Context context) {
        String line = alert.toString();
        System.err.println(line);   // console alert channel (stderr)
        fileWriter.println(line);   // persistent alert log
        fileWriter.flush();         // alerts must never sit in a buffer
    }

    @Override
    public void close() {
        if (fileWriter != null) {
            fileWriter.close();
        }
    }
}
