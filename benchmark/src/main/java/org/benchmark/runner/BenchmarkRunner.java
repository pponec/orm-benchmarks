package org.benchmark.runner;

import org.benchmark.common.Stopwatch;
import org.benchmark.exposed.ExposedBenchmark;
import org.benchmark.hibernate.HibernateBenchmark;
import org.benchmark.jdbi.JdbiBenchmark;
import org.benchmark.ujorm.UjormBenchmark;

/** Main execution class for all benchmarks */
public class BenchmarkRunner {

    /** Starts the execution of all framework tests */
    public static void main(String[] args) {
        System.out.println("Starting ORM Benchmarks...");
        var runner = new BenchmarkRunner();

        {   // Warming Up:
            var iterations = 1;
            runner.runHibernate("Hibernate", iterations);
            runner.runJdbi("Jdbi", iterations);
            runner.runExposed("Exposed", iterations);
            runner.runUjorm("Ujorm", iterations);
        }

        {   // Run The Benchmark:
            var iterations = 100_000;
            runner.runHibernate("Hibernate", iterations);
            runner.runJdbi("Jdbi", iterations);
            runner.runExposed("Exposed", iterations);
            runner.runUjorm("Ujorm", iterations);
        }
        System.out.println("All benchmarks finished. Results appended to ~/ujo-benchmark.csv");
    }

    /** Runs Hibernate benchmarks */
    public void runHibernate(String name, int iterations) {
        System.out.println("Running %s ...".formatted(name));
        var hibernate = new HibernateBenchmark();
        hibernate.testBatchInsert(new Stopwatch("Batch Insert", name, iterations));
        hibernate.testSpecificUpdate(new Stopwatch("Specific Update", name, iterations));
        hibernate.testRandomUpdate(new Stopwatch("Random Update", name, iterations));
        hibernate.testReadWithRelations(new Stopwatch("Read With Relations", name, iterations));
    }

    /** Runs JDBI benchmarks */
    public void runJdbi(String name, int iterations) {
        System.out.println("Running %s ...".formatted(name));
        var jdbi = new JdbiBenchmark();

        jdbi.testBatchInsert(new Stopwatch("Batch Insert", name, iterations));
        jdbi.testSpecificUpdate(new Stopwatch("Specific Update", name, iterations));
        jdbi.testRandomUpdate(new Stopwatch("Random Update", name, iterations));
        jdbi.testReadWithRelations(new Stopwatch("Read With Relations", name, iterations));
    }

    /** Runs Exposed benchmarks */
    public void runExposed(String name, int iterations) {
        System.out.println("Running %s ...".formatted(name));
        var exposed = new ExposedBenchmark();

        exposed.testBatchInsert(new Stopwatch("Batch Insert", name, iterations));
        exposed.testSpecificUpdate(new Stopwatch("Specific Update", name, iterations));
        exposed.testRandomUpdate(new Stopwatch("Random Update", name, iterations));
        exposed.testReadWithRelations(new Stopwatch("Read With Relations", name, iterations));
    }

    /** Runs Exposed benchmarks */
    public void runUjorm(String name, int iterations) {
        System.out.println("Running %s ...".formatted(name));
        var ujorm = new UjormBenchmark();

        ujorm.testBatchInsert(new Stopwatch("Batch Insert", name, iterations));
        ujorm.testSpecificUpdate(new Stopwatch("Specific Update", name, iterations));
        ujorm.testRandomUpdate(new Stopwatch("Random Update", name, iterations));
        ujorm.testReadWithRelations(new Stopwatch("Read With Relations", name, iterations));
    }
}