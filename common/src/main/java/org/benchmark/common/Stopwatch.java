package org.benchmark.common;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/** Utility class to measure and log benchmark times */
public class Stopwatch {

    private static final LocalDateTime START = LocalDateTime.now();
    private static final DateTimeFormatter MINUTE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'_'HH:mm");

    private final int iterations;
    private long startTime;
    private long endTime;

    /** Creates a new stopwatch with the specified number of iterations */
    public Stopwatch(int iterations) {
        this.iterations = iterations;
    }

    /** Gets the number of iterations for the benchmark */
    public int getIterations() {
        return iterations;
    }

    /** Executes the given runnable and measures its execution time */
    public void benchmark(Runnable task) {
        start();
        try {
            task.run();
        } finally {
            stop();
        }
    }

    /** Starts the stopwatch */
    private void start() {
        this.startTime = System.nanoTime();
    }

    /** Stops the stopwatch */
    private void stop() {
        this.endTime = System.nanoTime();
    }

    /** Returns the formatted duration of the measured task */
    public String getFormattedDuration() {
        var durationNanos = this.endTime - this.startTime;
        return formatDuration(durationNanos);
    }

    /** Gets the formatted start time */
    public static String getFormattedStart() {
        return START.format(MINUTE_FORMATTER);
    }

    /** Formats an integer to string with underscore thousands separators */
    public static String formatInteger(long number) {
        return String.format(Locale.US, "%,d", number).replace(',', '_');
    }

    /** Formats duration in nanoseconds to a string in seconds with 3 decimal places */
    public static String formatDuration(long durationNanos) {
        var durationSeconds = durationNanos / 1_000_000_000.0;
        return String.format(Locale.US, "%.3f", durationSeconds);
    }
}