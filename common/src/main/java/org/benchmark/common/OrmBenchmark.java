package org.benchmark.common;

import java.util.List;

/** Common interface for all ORM benchmarks */
public interface OrmBenchmark {

    /**
     * Measures insert performance for single-row persistence.
     * Implementations should include actual write work in measured scope
     * (e.g., SQL execution, flush and commit where applicable).
     * Setup and validation should run outside of the measured block.
     */
    int testSingleInsert(Stopwatch stopwatch);

    int testBatchInsert(Stopwatch stopwatch);

    int testSpecificUpdate(Stopwatch stopwatch);

    int testRandomUpdate(Stopwatch stopwatch);

    List<EmployeeRelationView> testReadWithRelations(Stopwatch stopwatch);

    List<?> testReadRelatedEntities(Stopwatch stopwatch);
}