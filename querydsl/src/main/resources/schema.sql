CREATE TABLE IF NOT EXISTS city (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    country_code VARCHAR(10),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    created_by VARCHAR(255),
    updated_by VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS employee (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    superior_id BIGINT,
    city_id BIGINT NOT NULL,
    contract_day DATE,
    is_active BOOLEAN,
    email VARCHAR(255),
    phone VARCHAR(255),
    salary DECIMAL(15,2),
    department VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    created_by VARCHAR(255),
    updated_by VARCHAR(255),
    FOREIGN KEY (superior_id) REFERENCES employee(id),
    FOREIGN KEY (city_id) REFERENCES city(id)
);