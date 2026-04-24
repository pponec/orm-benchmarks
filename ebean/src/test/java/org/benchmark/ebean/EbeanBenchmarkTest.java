package org.benchmark.ebean;

import org.benchmark.common.Stopwatch;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EbeanBenchmarkTest {

    @Test
    void testSpecificUpdateDoesNotThrowAndUpdatesInsertedRows() {
        try (var benchmark = new EbeanBenchmark()) {
            benchmark.testSingleInsert(new Stopwatch(5));

            var updatedCount = assertDoesNotThrow(() -> benchmark.testSpecificUpdate(new Stopwatch(1)));

            assertTrue(updatedCount >= 5);
        }
    }

    @Test
    void testReadWithRelationsDoesNotThrowAndReturnsRows() {
        try (var benchmark = new EbeanBenchmark()) {
            benchmark.testSingleInsert(new Stopwatch(5));

            var rows = assertDoesNotThrow(() -> benchmark.testReadWithRelations(new Stopwatch(1)));

            assertFalse(rows.isEmpty());
        }
    }
}
