BEGIN;

DROP TABLE IF EXISTS public.cities;
DROP TABLE IF EXISTS public.clients;


CREATE TABLE public.cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE public.clients (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    city_id INTEGER,
    FOREIGN KEY (city_id) REFERENCES cities(id)
);

INSERT INTO cities (name) VALUES ('New York'), ('Los Angeles'), ('Chicago');

INSERT INTO clients (name, email, city_id) VALUES
('John Doe', 'john.doe@example.com', 1),
('Jane Smith', 'jane.smith@example.com', 2),
('Alice Johnson', 'alice.johnson@example.com', 3);