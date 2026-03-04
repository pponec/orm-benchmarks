package org.ujorm.common;


import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

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
        this.startTime = System.currentTimeMillis();
    }

    /** Stops the stopwatch and appends the result to a CSV file */
    public void stop() {
        var endTime = System.currentTimeMillis();
        saveToCsv(endTime);
    }

    private void saveToCsv(long endTime) {
        var userHome = System.getProperty("user.home");
        var filePath = Path.of(userHome, "ujo-benchmark.csv");

        try (var writer = new PrintWriter(new FileWriter(filePath.toFile(), true))) {
            writer.printf("%s,%s,%d,%d,%d%n", testName, libraryName, iterations, startTime, endTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}