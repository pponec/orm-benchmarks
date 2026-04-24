package org.benchmark.common;

import java.util.List;

/** Common interface for all ORM benchmarks */
public interface OrmBenchmark {

    int testSingleInsert(Stopwatch stopwatch);

    int testBatchInsert(Stopwatch stopwatch);

    int testSpecificUpdate(Stopwatch stopwatch);

    int testRandomUpdate(Stopwatch stopwatch);

    List<EmployeeRelationView> testReadWithRelations(Stopwatch stopwatch);

    List<?> testReadRelatedEntities(Stopwatch stopwatch);
}