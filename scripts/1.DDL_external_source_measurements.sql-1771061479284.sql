CREATE SCHEMA IF NOT EXISTS dwh;

--Измерения

-- Клиенты 
DROP TABLE IF EXISTS dwh.dim_customer CASCADE;
CREATE TABLE dwh.dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    customer_email VARCHAR(100),
    birth_date DATE,
    customer_address TEXT,
    valid_from DATE NOT NULL,
    valid_to text,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, valid_from)
);

-- Мастера 
DROP TABLE IF EXISTS dwh.dim_craftsman CASCADE;
CREATE TABLE dwh.dim_craftsman (
    craftsman_sk SERIAL PRIMARY KEY,
    craftsman_id INTEGER NOT NULL,
    craftsman_name VARCHAR(100) NOT NULL,
    craftsman_email VARCHAR(100),
    craftsman_birthday DATE,
    craftsman_address TEXT,
    valid_from DATE NOT NULL,
    valid_to text,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(craftsman_id, valid_from)
);

-- Товары 
DROP TABLE IF EXISTS dwh.dim_product CASCADE;
CREATE TABLE dwh.dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    product_description TEXT,
    product_type VARCHAR(100),
    product_category VARCHAR(100) DEFAULT 'other',
    unit_price NUMERIC(10, 2) NOT NULL,
    valid_from DATE NOT NULL,
    valid_to text,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, valid_from)
);

-- Даты
DROP TABLE IF EXISTS dwh.dim_date CASCADE;
CREATE TABLE dwh.dim_date (
    date_sk SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    week INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE
);

-- Статусы заказов
DROP TABLE IF EXISTS dwh.dim_order_status CASCADE;
CREATE TABLE dwh.dim_order_status (
    status_sk SERIAL PRIMARY KEY,
    status_code VARCHAR(20) NOT NULL UNIQUE,
    status_name VARCHAR(50),
    status_group VARCHAR(20), 
    is_final BOOLEAN DEFAULT FALSE,
    description TEXT
);

-- Факты
-- Заказы
DROP TABLE IF EXISTS dwh.fact_order_sales CASCADE;
CREATE TABLE dwh.fact_order_sales (
    order_sale_sk BIGSERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    line_item_id INTEGER,
    customer_sk INTEGER REFERENCES dwh.dim_customer(customer_sk),
    craftsman_sk INTEGER REFERENCES dwh.dim_craftsman(craftsman_sk),
    product_sk INTEGER REFERENCES dwh.dim_product(product_sk),
    order_date_sk INTEGER REFERENCES dwh.dim_date(date_sk),
    completion_date_sk INTEGER REFERENCES dwh.dim_date(date_sk),
    status_sk INTEGER REFERENCES dwh.dim_order_status(status_sk),
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price NUMERIC(10, 2) NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1. Загрузка клиентов
INSERT INTO dwh.dim_customer (
    customer_id,
    customer_name,
    customer_email,
    birth_date,
    customer_address,
    valid_from,
    valid_to,
    is_current
)
SELECT 
    customer_id,
    customer_name,
    customer_email,
    customer_birthday AS birth_date,
    customer_address,
    CURRENT_DATE AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM external_source.customers;

-- 2. Загрузка мастеров
INSERT INTO dwh.dim_craftsman (
    craftsman_id,
    craftsman_name,
    craftsman_email,
    craftsman_birthday,
    craftsman_address,
    valid_from,
    valid_to,
    is_current
)
SELECT DISTINCT
    craftsman_id,
    craftsman_name,
    craftsman_email,
    craftsman_birthday,
    craftsman_address,
    CURRENT_DATE AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM external_source.craft_products_orders;

-- 3. Загрузка товаров
INSERT INTO dwh.dim_product (
    product_id,
    product_name,
    product_description,
    product_type,
    product_category,
    unit_price,
    valid_from,
    valid_to,
    is_current
)
SELECT DISTINCT
    product_id,
    product_name,
    product_description,
    COALESCE(product_type, 'other') AS product_type,
    'other' AS product_category,
    product_price AS unit_price,
    CURRENT_DATE AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM external_source.craft_products_orders;

-- 4. Загрузка дат 
INSERT INTO dwh.dim_date (
    full_date,
    year,
    quarter,
    month,
    month_name,
    week,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend
)
SELECT DISTINCT
    order_created_date::DATE AS full_date,
    EXTRACT(YEAR FROM order_created_date)::INTEGER AS year,
    EXTRACT(QUARTER FROM order_created_date)::INTEGER AS quarter,
    EXTRACT(MONTH FROM order_created_date)::INTEGER AS month,
    TO_CHAR(order_created_date, 'Month') AS month_name,
    EXTRACT(WEEK FROM order_created_date)::INTEGER AS week,
    EXTRACT(DAY FROM order_created_date)::INTEGER AS day_of_month,
    EXTRACT(DOW FROM order_created_date)::INTEGER AS day_of_week,
    TO_CHAR(order_created_date, 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM order_created_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM external_source.craft_products_orders
WHERE order_created_date IS NOT NULL
UNION
SELECT DISTINCT
    order_completion_date::DATE,
    EXTRACT(YEAR FROM order_completion_date)::INTEGER,
    EXTRACT(QUARTER FROM order_completion_date)::INTEGER,
    EXTRACT(MONTH FROM order_completion_date)::INTEGER,
    TO_CHAR(order_completion_date, 'Month'),
    EXTRACT(WEEK FROM order_completion_date)::INTEGER,
    EXTRACT(DAY FROM order_completion_date)::INTEGER,
    EXTRACT(DOW FROM order_completion_date)::INTEGER,
    TO_CHAR(order_completion_date, 'Day'),
    CASE WHEN EXTRACT(DOW FROM order_completion_date) IN (0, 6) THEN TRUE ELSE FALSE END
FROM external_source.craft_products_orders
WHERE order_completion_date IS NOT NULL
ORDER BY full_date;

-- 5. Загрузка статусов

INSERT INTO dwh.dim_order_status (
    status_code,
    status_name,
    status_group,
    is_final,
    description
)
VALUES 
    ('created', 'Создан', 'ACTIVE', FALSE, 'Заказ создан'),
    ('in progress', 'В работе', 'ACTIVE', FALSE, 'Заказ в процессе выполнения'),
    ('delivery', 'Доставка', 'ACTIVE', FALSE, 'Заказ передан в доставку'),
    ('completed', 'Завершен', 'COMPLETED', TRUE, 'Заказ успешно завершен'),
    ('cancelled', 'Отменен', 'CANCELLED', TRUE, 'Заказ отменен');

-- 6. Загрузка фактов

INSERT INTO dwh.fact_order_sales (
    order_id,
    line_item_id,
    customer_sk,
    craftsman_sk,
    product_sk,
    order_date_sk,
    completion_date_sk,
    status_sk,
    quantity,
    unit_price,
    total_amount
)
SELECT 
    cpo.order_id,
    ROW_NUMBER() OVER (PARTITION BY cpo.order_id ORDER BY cpo.product_id) AS line_item_id,
    dc.customer_sk,
    dcr.craftsman_sk,
    dp.product_sk,
    dd_order.date_sk AS order_date_sk,
    dd_compl.date_sk AS completion_date_sk,
    dos.status_sk,
    1 AS quantity,  -- В данных нет количества, ставим 1 по умолчанию
    cpo.product_price AS unit_price,
    cpo.product_price AS total_amount  -- total_amount = unit_price * 1
FROM external_source.craft_products_orders cpo
LEFT JOIN dwh.dim_customer dc 
    ON cpo.customer_id = dc.customer_id 
    AND dc.is_current = TRUE
LEFT JOIN dwh.dim_craftsman dcr 
    ON cpo.craftsman_id = dcr.craftsman_id 
    AND dcr.is_current = TRUE
LEFT JOIN dwh.dim_product dp 
    ON cpo.product_id = dp.product_id 
    AND dp.is_current = TRUE
LEFT JOIN dwh.dim_date dd_order 
    ON cpo.order_created_date::DATE = dd_order.full_date
LEFT JOIN dwh.dim_date dd_compl 
    ON cpo.order_completion_date::DATE = dd_compl.full_date
LEFT JOIN dwh.dim_order_status dos 
    ON cpo.order_status = dos.stat