package org.benchmark.common;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/** Utility class to measure and log benchmark times */
public class Stopwatch {

    private final String testName;
    private final String libraryName;
    private final int iterations;
    private long startTime;

    public Stopwatch(String testName, String libraryName, int iterations) {
        this.testName = testName;
        this.libraryName = libraryName;
        this.iterations = iterations;
    }

    /** Starts the stopwatch */
    public void start() {
        System.gc();
        this.startTime = System.nanoTime();
    }

    /** Stops the stopwatch and appends the result to a CSV file */
    public void stop() {
        var endTime = System.nanoTime();
        saveToCsv(endTime);
    }

    private void saveToCsv(long endTime) {
        if (iterations <= 100) {
            System.out.println("Warming Up: %s -> %s".formatted(libraryName, testName));
            return;
        }

        var userHome = System.getProperty("user.home");
        var filePath = Path.of(userHome, "ujo-benchmark.csv");

        try {
            var isNewFile = !Files.exists(filePath) || Files.size(filePath) == 0;

            try (var writer = new PrintWriter(new FileWriter(filePath.toFile(), true))) {
                if (isNewFile) {
                    writer.println("Library|Test Name|Iterations|Duration (s)");
                }

                var durationNanos = endTime - this.startTime;
                var formattedDuration = formatDuration(durationNanos);
                var formattedIterations = formatInteger(iterations);

                writer.printf("%s|%s|%s|%s%n", libraryName, testName, formattedIterations, formattedDuration);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Formats an integer to string with underscore thousands separators */
    public static String formatInteger(long number) {
        var result = String.format(Locale.US, "%,d", number).replace(',', '_');
        return result;
    }

    /** Formats duration in nanoseconds to custom string with underscores */
    public static String formatDuration(long durationNanos) {
        var seconds = durationNanos / 1_000_000_000L;
        var nanos = durationNanos % 1_000_000_000L;

        var secString = formatInteger(seconds);
        var nanoString = String.format(Locale.US, "%09d", nanos);
        var fractionString = nanoString.substring(0, 3) + "_" + nanoString.substring(3, 6) + "_" + nanoString.substring(6, 9);

        var result = secString + "." + fractionString;
        return result;
    }
}