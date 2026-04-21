package org.benchmark.common;

/** Common interface for all ORM benchmarks */
public interface OrmBenchmark {

    void testSingleInsert(Stopwatch stopwatch);

    void testBatchInsert(Stopwatch stopwatch);

    void testSpecificUpdate(Stopwatch stopwatch);

    void testRandomUpdate(Stopwatch stopwatch);

    void testReadWithRelations(Stopwatch stopwatch);

    void testReadRelatedEntities(Stopwatch stopwatch);
}