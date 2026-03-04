# ORM Benchmark

This project compares the performance of several Java/Kotlin ORM and database mapping frameworks.

## Environment
* **Java Version:** Java 25
* **Database:** H2 Database in-memory (version 2.4.240)
* **Hardware/Memory:** ~15 GiB total RAM available (tested on a machine with 15 GiB RAM and 4 GiB swap)

## Tested Frameworks
* **Hibernate Core:** `7.0.0.Final`
* **JDBI3 Core:** `3.51.0`
* **Exposed:** `0.58.0`
* **Ujorm3:** *(coming soon)*

## Test Scenarios
All tests exclude the initial warm-up phase to ensure accurate JIT compilation and memory allocation measurements.
* **Batch Insert:** Inserts generated employee records into the database using JDBC batching (batch size of 50).
* **Specific Update:** Updates specific columns (`salary` and `updated_at`) for all existing employee records in a single transaction.
* **Random Update:** Iterates through all employees and randomly modifies either their `is_active` status or `department` name to simulate unpredictable workload, saved in batches.
* **Read With Relations:** Retrieves all employee records along with their associated `City` and superior `Employee` entities using SQL JOINs (preventing the N+1 select problem).

## Current Results (Temporary)

| Library | Test Name | Iterations | Duration (s) |
| :--- | :--- | :--- | :--- |
| Hibernate | Batch Insert | 10_000 | 1.049_739_083 |
| Hibernate | Specific Update | 10_000 | 3.889_216_183 |
| Hibernate | Random Update | 10_000 | 3.961_171_153 |
| Hibernate | Read With Relations | 10_000 | 0.875_579_461 |
| Jdbi | Batch Insert | 10_000 | 0.600_614_325 |
| Jdbi | Specific Update | 10_000 | 5.040_277_969 |
| Jdbi | Random Update | 10_000 | 3.204_188_552 |
| Jdbi | Read With Relations | 10_000 | 0.152_702_103 |
| Exposed | Batch Insert | 10_000 | 1.061_643_570 |
| Exposed | Specific Update | 10_000 | 4.764_172_959 |
| Exposed | Random Update | 10_000 | 4.449_776_576 |
| Exposed | Read With Relations | 10_000 | 0.709_159_491 |
| Ujorm | Batch Insert | 10_000 | 0.356_480_640 |
| Ujorm | Specific Update | 10_000 | 4.000_694_851 |
| Ujorm | Random Update | 10_000 | 4.081_079_634 |
| Ujorm | Read With Relations | 10_000 | 0.243_207_528 |

---

