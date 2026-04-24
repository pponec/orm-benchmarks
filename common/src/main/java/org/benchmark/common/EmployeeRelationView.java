package org.benchmark.common;

/** Data transfer object for employee relations */
public record EmployeeRelationView(
        /** Gets the ID of the employee. */
        Long id,
        /** Gets the name of the employee. */
        String name,
        /** Gets the name of the city. */
        String cityName,
        /** Gets the name of the superior. */
        String superiorName
) {
}