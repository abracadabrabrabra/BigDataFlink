DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_pet CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_customer (
    customer_key VARCHAR(16) PRIMARY KEY,
    customer_first_name VARCHAR(200),
    customer_last_name VARCHAR(200),
    customer_age SMALLINT,
    customer_email VARCHAR(255) UNIQUE,
    customer_country VARCHAR(100),
    customer_postal_code VARCHAR(20)
);

CREATE TABLE dim_seller (
    seller_key VARCHAR(16) PRIMARY KEY,
    seller_first_name VARCHAR(200),
    seller_last_name VARCHAR(200),
    seller_email VARCHAR(255) UNIQUE,
    seller_country VARCHAR(100),
    seller_postal_code VARCHAR(20)
);

CREATE TABLE dim_store (
    store_key VARCHAR(16) PRIMARY KEY,
    store_name VARCHAR(500),
    store_location VARCHAR(200),
    store_city VARCHAR(100),
    store_state VARCHAR(50),
    store_country VARCHAR(100),
    store_phone VARCHAR(30),
    store_email VARCHAR(255),
    UNIQUE(store_name, store_city, store_country)
);

CREATE TABLE dim_supplier (
    supplier_key VARCHAR(16) PRIMARY KEY,
    supplier_name VARCHAR(500),
    supplier_contact VARCHAR(200),
    supplier_email VARCHAR(255),
    supplier_phone VARCHAR(30),
    supplier_address TEXT,
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100),
    UNIQUE(supplier_name, supplier_email)
);

CREATE TABLE dim_product (
    product_key VARCHAR(16) PRIMARY KEY,
    product_name VARCHAR(500),
    product_category VARCHAR(100),
    product_brand VARCHAR(200),
    product_color VARCHAR(50),
    product_size VARCHAR(20),
    product_material VARCHAR(100),
    product_weight NUMERIC(10,2),
    product_description TEXT,
    pet_category VARCHAR(50),
    supplier_key VARCHAR(16),
    UNIQUE(product_name, product_brand, product_size, product_color, supplier_key)
);

CREATE TABLE dim_pet (
    pet_key VARCHAR(16) PRIMARY KEY,
    pet_type VARCHAR(50),
    pet_name VARCHAR(100),
    pet_breed VARCHAR(100),
    customer_key VARCHAR(16),
    UNIQUE(pet_type, pet_name, pet_breed, customer_key)
);

CREATE TABLE dim_date (
    date_key VARCHAR(16) PRIMARY KEY,
    full_date DATE UNIQUE,
    year INTEGER,
    month INTEGER,
    day INTEGER
);

CREATE TABLE fact_sales (
    sale_key SERIAL PRIMARY KEY,
    customer_key VARCHAR(16) NOT NULL,
    seller_key VARCHAR(16) NOT NULL,
    product_key VARCHAR(16) NOT NULL,
    store_key VARCHAR(16) NOT NULL,
    date_key VARCHAR(16) NOT NULL,
    pet_key VARCHAR(16),
    quantity SMALLINT,
    total_price NUMERIC(12,2),
    unit_price NUMERIC(12,2),
    product_rating NUMERIC(3,1),
    product_reviews INTEGER,
    stock_quantity INTEGER,
    product_release_date DATE,
    product_expiry_date DATE
);