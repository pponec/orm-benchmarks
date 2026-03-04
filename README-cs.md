# ORM Benchmark (Česky)

Tento projekt porovnává výkon několika ORM a databázových frameworků pro Javu a Kotlin.

## Prostředí
* **Verze Javy:** Java 25
* **Databáze:** H2 Database in-memory (verze 2.4.240)
* **Hardware/Paměť:** ~15 GiB celkové dostupné paměti RAM (testováno na stroji s 15 GiB RAM a 4 GiB swap)

## Testované frameworky
* **Hibernate Core:** `7.0.0.Final`
* **JDBI3 Core:** `3.51.0`
* **Exposed:** `0.58.0`
* **Ujorm3:** *(připravuje se)*

## Testovací scénáře
Všechny testy ignorují počáteční zahřívací fázi (warm-up), aby bylo zajištěno přesné měření po JIT kompilaci a bez zkreslení alokací paměti.
* **Batch Insert:** Vkládá vygenerované záznamy zaměstnanců do databáze pomocí dávkového zpracování (velikost dávky 50).
* **Specific Update:** Aktualizuje konkrétní sloupce (`salary` a `updated_at`) pro všechny existující záznamy zaměstnanců v rámci jediné transakce.
* **Random Update:** Prochází všechny zaměstnance a náhodně jim mění buď stav `is_active`, nebo název oddělení `department`. Simuluje tak nepředvídatelnou zátěž; ukládá se v dávkách.
* **Read With Relations:** Načte všechny záznamy zaměstnanců společně s navázanými entitami města (`City`) a nadřízeného (`Employee`) pomocí SQL JOINů (zamezení problému N+1 selectů).

## Aktuální výsledky (Dočasné)

| Library | Test Name | Iterations | Duration (s) | JAR Size | 
| :--- | :--- | :--- | :--- |:---------|
| Hibernate | Batch Insert | 10_000 | 1.049_739_083 | 25.68 MB |
| Hibernate | Specific Update | 10_000 | 3.889_216_183 | |
| Hibernate | Random Update | 10_000 | 3.961_171_153 | |
| Hibernate | Read With Relations | 10_000 | 0.875_579_461 | |
| Jdbi | Batch Insert | 10_000 | 0.600_614_325 | 3.89 MB  |
| Jdbi | Specific Update | 10_000 | 5.040_277_969 | |
| Jdbi | Random Update | 10_000 | 3.204_188_552 | |
| Jdbi | Read With Relations | 10_000 | 0.152_702_103 | |
| Exposed | Batch Insert | 10_000 | 1.061_643_570 | 9.80 MB  |
| Exposed | Specific Update | 10_000 | 4.764_172_959 | |
| Exposed | Random Update | 10_000 | 4.449_776_576 | |
| Exposed | Read With Relations | 10_000 | 0.709_159_491 | |
| Ujorm | Batch Insert | 10_000 | 0.356_480_640 | 2.74 MB  |
| Ujorm | Specific Update | 10_000 | 4.000_694_851 | |
| Ujorm | Random Update | 10_000 | 4.081_079_634 | |
| Ujorm | Read With Relations | 10_000 | 0.243_207_528 | |
