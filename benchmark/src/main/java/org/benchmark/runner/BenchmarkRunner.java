package org.benchmark.runner;

import lombok.RequiredArgsConstructor;
import org.benchmark.common.OrmBenchmark;
import org.benchmark.common.Stopwatch;
import org.benchmark.exposed.ExposedBenchmark;
import org.benchmark.hibernate.HibernateBenchmark;
import org.benchmark.jdbi.JdbiBenchmark;
import org.benchmark.jooq.JooqBenchmark;
import org.benchmark.mybatis.MyBatisBenchmark;
import org.benchmark.querydsl.QuerydslSqlBenchmark;
import org.benchmark.springdatajdbc.SpringDataJdbcBenchmark;
import org.benchmark.ujorm.UjormBenchmark;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Main execution class for all benchmarks */
public class BenchmarkRunner {

    /** The report file name */
    public static final String CSV_FILE = "ujo-benchmark.csv";

    /** Enum representing the tested framework */
    @RequiredArgsConstructor
    public enum Framework {
        HIBERNATE("Hibernate", "hibernate", HibernateBenchmark::new),
        JDBI("Jdbi", "jdbi", JdbiBenchmark::new),
        EXPOSED("Exposed", "exposed", ExposedBenchmark::new),
        MYBATIS("MyBatis", "mybatis", MyBatisBenchmark::new),
        QUERYDSL("QueryDsl", "querydsl", QuerydslSqlBenchmark::new),
        JOOQ("Jooq", "jooq", JooqBenchmark::new),
        UJORM("Ujorm3", "ujorm3", UjormBenchmark::new);

        /** CSV label */
        private final String label;
        /** Maven artifact ID prefix for JAR size lookup */
        private final String jarPrefix;
        /** Test object provider */
        private final Supplier<OrmBenchmark> supplier;

        /** Gets label */
        public String getLabel() { return label; }
        /** Gets JAR prefix */
        public String getJarPrefix() { return jarPrefix; }
        /** Gets supplier */
        public Supplier<OrmBenchmark> getSupplier() { return supplier; }
    }

    /** Enum representing the benchmark operation */
    public enum Operation {
        BATCH_INSERT("Batch Insert", OrmBenchmark::testBatchInsert),
        SPECIFIC_UPDATE("Specific Update", OrmBenchmark::testSpecificUpdate),
        RANDOM_UPDATE("Random Update", OrmBenchmark::testRandomUpdate),
        READ_WITH_RELATIONS("Read With Relations", OrmBenchmark::testReadWithRelations),
        READ_ENTITY_RELATIONS("Read Related Entities", OrmBenchmark::testReadRelatedEntities);

        private final String label;
        private final BiConsumer<OrmBenchmark, Stopwatch> action;

        /** Constructor */
        Operation(String label, BiConsumer<OrmBenchmark, Stopwatch> action) {
            this.label = label;
            this.action = action;
        }

        /** Gets label */
        public String getLabel() { return label; }
        /** Gets action */
        public BiConsumer<OrmBenchmark, Stopwatch> getAction() { return action; }
    }

    /** Calculates the exact size of the framework's jar-with-dependencies file in MB */
    private static String getJarSizeMb(String jarPrefix) {
        try (var paths = Files.walk(Path.of("."))) {
            var totalSizeBytes = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        var fileName = p.getFileName().toString();
                        return fileName.contains("jar-with-dependencies")
                                && fileName.endsWith(".jar")
                                && fileName.startsWith(jarPrefix);
                    })
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            return 0L;
                        }
                    })
                    .sum();
            if (totalSizeBytes == 0) {
                return "?";
            }
            return String.format(Locale.US, "%.2f", totalSizeBytes / Math.pow(2, 20));
        } catch (IOException e) {
            return "?";
        }
    }

    /**
     * Gets the total number of bytes allocated by the current thread.
     * This method measures the "Allocation Rate" - it counts every single object
     * created on the Heap, even if it is immediately destroyed by the Garbage Collector.
     * It is used to measure the framework's memory efficiency and GC pressure.
     */
    private static long getThreadAllocatedBytes() {
        var bean = java.lang.management.ManagementFactory.getThreadMXBean();
        if (bean instanceof com.sun.management.ThreadMXBean sunBean) {
            return sunBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        }
        return 0L;
    }

    /** Starts the execution of all framework tests */
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Error: Target framework name is required as the first argument.");
            return;
        }

        var framework = Framework.valueOf(args[0].toUpperCase());
        var iterations = (args.length > 1 && args[1].matches("\\d+"))
                ? Integer.parseInt(args[1])
                : 500_000;

        System.out.println("Starting ORM Benchmark for %s with %s iterations...".formatted(framework.getLabel(), Stopwatch.formatInteger(iterations)));

        var instance = framework.getSupplier().get();

        // 1. Warming Up phase
        for (var operation : Operation.values()) {
            System.out.println("Warming Up: %s -> %s".formatted(framework.getLabel(), operation.getLabel()));
            var warmupStopwatch = new Stopwatch(iterations - 1);
            operation.getAction().accept(instance, warmupStopwatch);
        }

        var durations = new ArrayList<String>();
        var memoryAllocations = new ArrayList<String>();

        // 2. Benchmark phase with precise Allocation Rate tracking
        for (var operation : Operation.values()) {
            System.out.println("Running...: %s -> %s".formatted(framework.getLabel(), operation.getLabel()));
            var actualStopwatch = new Stopwatch(iterations);

            var startAllocatedBytes = getThreadAllocatedBytes();
            operation.getAction().accept(instance, actualStopwatch);
            var endAllocatedBytes = getThreadAllocatedBytes();

            var allocatedBytesTotal = endAllocatedBytes - startAllocatedBytes;
            var bytesPerRecord = allocatedBytesTotal / iterations;

            durations.add(actualStopwatch.getFormattedDuration());
            memoryAllocations.add(Stopwatch.formatInteger(bytesPerRecord));
        }

        // 3. Calculate JAR size for the specific framework
        var jarSizeMb = getJarSizeMb(framework.getJarPrefix());

        saveToCsv(framework.getLabel(), iterations, durations, memoryAllocations, jarSizeMb);
        System.out.println("Benchmark finished. Results appended to ~/%s".formatted(CSV_FILE));
    }

    /** Appends the aggregated benchmark results to a CSV file */
    public static void saveToCsv(String libraryName, int iterations, List<String> durations, List<String> memoryAllocations, String jarSizeMb) {
        var userHome = System.getProperty("user.home");
        var filePath = Path.of(userHome, CSV_FILE);

        try {
            var isNewFile = !Files.exists(filePath) || Files.size(filePath) == 0;

            try (var writer = new PrintWriter(new FileWriter(filePath.toFile(), true))) {
                if (isNewFile) {
                    var headerOperations = java.util.Arrays.stream(Operation.values())
                            .map(op -> op.getLabel() + " [s]")
                            .collect(Collectors.joining("|"));

                    var headerMemories = java.util.Arrays.stream(Operation.values())
                            .map(op -> op.getLabel() + " Mem [B/op]")
                            .collect(Collectors.joining("|"));

                    writer.println("Start|Library|Iterations|" + headerOperations + "|" + headerMemories + "|JAR Size [MB]");
                }

                var formattedStart = Stopwatch.getFormattedStart();
                var formattedIterations = Stopwatch.formatInteger(iterations);
                var joinedDurations = String.join("|", durations);
                var joinedMemories = String.join("|", memoryAllocations);

                writer.printf("%s|%s|%s|%s|%s|%s%n", formattedStart, libraryName, formattedIterations, joinedDurations, joinedMemories, jarSizeMb);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}