CREATE TABLE IF NOT EXISTS people (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name  VARCHAR(100),
    email      VARCHAR(200),
    age        INT
);

CREATE TABLE IF NOT EXISTS enriched_people (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name  VARCHAR(100),
    email      VARCHAR(200),
    age        INT,
    full_name  VARCHAR(200),
    category   VARCHAR(20)
);
