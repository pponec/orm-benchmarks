package org.benchmark.ebean.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/** Employee entity mapping */
@Entity
@Table(name = "employee")
@Getter
@Setter
public class Employee {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String name;
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "superior_id")
    private Employee superior;
    @ManyToOne(optional = false)
    @JoinColumn(name = "city_id")
    private City city;
    @Column(name = "contract_day") private LocalDate contractDay;
    @Column(name = "is_active") private Boolean isActive;
    private String email;
    private String phone;
    private BigDecimal salary;
    private String department;
    @Column(name = "created_at") private LocalDateTime createdAt;
    @Column(name = "updated_at") private LocalDateTime updatedAt;
    @Column(name = "created_by") private String createdBy;
    @Column(name = "updated_by") private String updatedBy;
}