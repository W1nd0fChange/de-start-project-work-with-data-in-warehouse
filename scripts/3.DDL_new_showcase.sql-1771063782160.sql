-- лог для дат загрузки данных
DROP TABLE IF EXISTS dwh.load_log_customer_report CASCADE;
CREATE TABLE dwh.load_log_customer_report (
    load_id SERIAL PRIMARY KEY,
    load_date DATE NOT NULL DEFAULT CURRENT_DATE,
    report_year INTEGER NOT NULL,
    report_month INTEGER NOT NULL,
    records_loaded INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'SUCCESS',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Основная витрина по заказчикам
DROP TABLE IF EXISTS dwh.customer_report_datamart CASCADE;
CREATE TABLE dwh.customer_report_datamart (
    id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    customer_address TEXT,
    customer_birthday DATE,
    customer_email VARCHAR(100),
    customer_money NUMERIC(15, 2),
    platform_money NUMERIC(15, 2),
    count_order INTEGER,
    avg_price_order NUMERIC(10, 2),
    median_time_order_completed INTEGER,
    top_product_category VARCHAR(100),
    top_craftsman_id INTEGER,
    count_order_created INTEGER,
    count_order_in_progress INTEGER,
    count_order_delivery INTEGER,
    count_order_done INTEGER,
    count_order_not_done INTEGER,
    report_year INTEGER NOT NULL,
    report_month INTEGER NOT NULL,
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, report_year, report_month)
);