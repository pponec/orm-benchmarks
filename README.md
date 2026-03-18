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
* **MyBatis:** `3.5.15`
* **Ujorm3:** `3.0.0-beta`

## Test Scenarios & Metrics
All tests exclude the initial warm-up phase to ensure accurate JIT compilation and memory allocation measurements. The default number of iterations is **500,000**. The table columns represent the following metrics:

* **Batch Insert [s]:** The total time taken to insert generated employee records into the database using JDBC batching (batch size of 50). Lower is better.
* **Specific Update [s]:** The total time taken to update specific columns (`salary` and `updated_at`) for all existing employee records in a single transaction. Lower is better.
* **Random Update [s]:** The total time taken to iterate through all employees and randomly modify either their `is_active` status or `department` name to simulate an unpredictable workload, saved in batches. Lower is better.
* **Read Rel. [s]:** The total time taken to retrieve all employee records along with their associated `City` and superior `Employee` entities using SQL JOINs (preventing the N+1 select problem). Lower is better.
* **Mem [B/op] (Allocation Rate):** The total amount of temporary Heap memory (garbage) created by the framework to process a single record. It is measured using the JVM's internal `ThreadMXBean`. Lower allocation means less pressure on the Garbage Collector, resulting in lower CPU usage, reduced latency spikes, and faster execution times.
* **JAR Size [MB]:** The total file size of the compiled `jar-with-dependencies` archive. Lower is better.

## Benchmark Results

| Library | Batch<br/>Insert [s] | Specific<br/>Update [s] | Random<br/>Update [s] | Read Rel.<br/>[s] | Insert Mem<br/>[B/op] | Update Mem<br/>[B/op] | Rand Upd<br/>Mem [B/op] | Read Mem<br/>[B/op] | JAR Size<br/>[MB] |
|:--------|---------------------:|------------------------:|----------------------:|------------------:|----------------------:|----------------------:|------------------------:|--------------------:|------------------:|
| Hibernate | 2.584 | 8.153 | 8.108 | **0.311** | 16_381 | 53_269 | 53_207 | **1_158** | 25.68 |
| Jdbi | 1.614 | 8.721 | 4.460 | 0.380 | 13_666 | 54_931 | 40_031 | 1_638 | 3.89 |
| Exposed | 3.460 | 6.665 | 6.103 | 1.134 | 23_363 | 49_312 | 47_125 | 4_051 | 9.80 |
| MyBatis | 1.591 | 5.101 | 5.198 | 0.588 | 12_794 | 38_854 | 38_689 | 3_478 | 4.27 |
| Ujorm3 | **1.269** | **3.849** | **4.143** | 0.413 | **11_874** | **37_478** | **37_963** | 1_705 | **2.75** |

---

Learn more about the [Ujorm3 framework](https://github.com/pponec/ujorm/tree/ujorm3?tab=readme-ov-file#-ujorm3-framework).