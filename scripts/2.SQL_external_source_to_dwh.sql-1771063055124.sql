 DROP TABLE IF EXISTS tmp_sources;
CREATE TEMP TABLE tmp_sources AS 
SELECT 
    cpo.order_id,
    cpo.order_created_date,
    cpo.order_completion_date,
    cpo.order_status,
    cpo.craftsman_id,
    cpo.craftsman_name,
    cpo.craftsman_address,
    cpo.craftsman_birthday,
    cpo.craftsman_email,
    cpo.product_id,
    cpo.product_name,
    cpo.product_description,
    cpo.product_type,
    cpo.product_price,
    cpo.customer_id,
    c.customer_name,
    c.customer_address,
    c.customer_birthday,
    c.customer_email
FROM external_source.craft_products_orders cpo
LEFT JOIN external_source.customers c ON cpo.customer_id = c.customer_id;

-- 1. Обновление мастеров

MERGE INTO dwh.dim_craftsman d
USING (
    SELECT DISTINCT 
        craftsman_id,
        craftsman_name,
        craftsman_address,
        craftsman_birthday,
        craftsman_email,
        CURRENT_DATE AS valid_from
    FROM tmp_sources
    WHERE craftsman_id IS NOT NULL
) t
ON d.craftsman_id = t.craftsman_id AND d.is_current = TRUE
WHEN MATCHED AND (
    d.craftsman_name != t.craftsman_name OR
    d.craftsman_address != t.craftsman_address OR
    d.craftsman_birthday != t.craftsman_birthday OR
    d.craftsman_email != t.craftsman_email
) THEN
    UPDATE SET 
        is_current = FALSE,
        valid_to = CURRENT_DATE - INTERVAL '1 day',
        updated_at = CURRENT_TIMESTAMP
;

-- Новые мастера 
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
FROM tmp_sources
WHERE craftsman_id IS NOT NULL
AND NOT EXISTS (
    SELECT 1 
    FROM dwh.dim_craftsman d 
    WHERE d.craftsman_id = tmp_sources.craftsman_id 
    AND d.is_current = TRUE
    AND d.craftsman_name = tmp_sources.craftsman_name
    AND d.craftsman_address = tmp_sources.craftsman_address
    AND d.craftsman_birthday = tmp_sources.craftsman_birthday
    AND d.craftsman_email = tmp_sources.craftsman_email
);

-- 2. Обновление товаров

MERGE INTO dwh.dim_product d
USING (
    SELECT DISTINCT 
        product_id,
        product_name,
        product_description,
        product_type,
        product_price AS unit_price,
        CURRENT_DATE AS valid_from
    FROM tmp_sources
    WHERE product_id IS NOT NULL
) t
ON d.product_id = t.product_id AND d.is_current = TRUE
WHEN MATCHED AND (
    d.product_name != t.product_name OR
    COALESCE(d.product_description, '') != COALESCE(t.product_description, '') OR
    d.product_type != t.product_type OR
    d.unit_price != t.unit_price
) THEN
    UPDATE SET 
        is_current = FALSE,
        valid_to = CURRENT_DATE - INTERVAL '1 day',
        updated_at = CURRENT_TIMESTAMP
;

-- новые товары
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
FROM tmp_sources
WHERE product_id IS NOT NULL
AND NOT EXISTS (
    SELECT 1 
    FROM dwh.dim_product d 
    WHERE d.product_id = tmp_sources.product_id 
    AND d.is_current = TRUE
    AND d.product_name = tmp_sources.product_name
    AND COALESCE(d.product_description, '') = COALESCE(tmp_sources.product_description, '')
    AND d.product_type = COALESCE(tmp_sources.product_type, 'other')
    AND d.unit_price = tmp_sources.product_price
);

-- 3. Обновление клиентов


MERGE INTO dwh.dim_customer d
USING (
    SELECT DISTINCT 
        customer_id,
        customer_name,
        customer_address,
        customer_birthday AS birth_date,
        customer_email,
        CURRENT_DATE AS valid_from
    FROM tmp_sources
    WHERE customer_id IS NOT NULL
) t
ON d.customer_id = t.customer_id AND d.is_current = TRUE
WHEN MATCHED AND (
    d.customer_name != t.customer_name OR
    d.customer_address != t.customer_address OR
    d.birth_date != t.birth_date OR
    d.customer_email != t.customer_email
) THEN
    UPDATE SET 
        is_current = FALSE,
        valid_to = CURRENT_DATE - INTERVAL '1 day',
        updated_at = CURRENT_TIMESTAMP
;

-- новые клиенрт 
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
SELECT DISTINCT
    customer_id,
    customer_name,
    customer_email,
    customer_birthday AS birth_date,
    customer_address,
    CURRENT_DATE AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM tmp_sources
WHERE customer_id IS NOT NULL
AND NOT EXISTS (
    SELECT 1 
    FROM dwh.dim_customer d 
    WHERE d.customer_id = tmp_sources.customer_id 
    AND d.is_current = TRUE
    AND d.customer_name = tmp_sources.customer_name
    AND d.customer_address = tmp_sources.customer_address
    AND d.birth_date = tmp_sources.customer_birthday
    AND d.customer_email = tmp_sources.customer_email
);

-- 4. Обновление статусов
MERGE INTO dwh.dim_order_status d
USING (
    SELECT DISTINCT 
        order_status AS status_code,
        CASE 
            WHEN order_status = 'created' THEN 'Создан'
            WHEN order_status = 'in progress' THEN 'В работе'
            WHEN order_status = 'delivery' THEN 'Доставка'
            WHEN order_status = 'completed' THEN 'Завершен'
            WHEN order_status = 'cancelled' THEN 'Отменен'
            ELSE order_status
        END AS status_name,
        CASE 
            WHEN order_status IN ('created', 'in progress', 'delivery') THEN 'ACTIVE'
            WHEN order_status = 'completed' THEN 'COMPLETED'
            WHEN order_status = 'cancelled' THEN 'CANCELLED'
            ELSE 'OTHER'
        END AS status_group,
        CASE WHEN order_status IN ('completed', 'cancelled') THEN TRUE ELSE FALSE END AS is_final
    FROM tmp_sources
    WHERE order_status IS NOT NULL
) t
ON d.status_code = t.status_code
WHEN NOT MATCHED THEN
    INSERT (status_code, status_name, status_group, is_final, description)
    VALUES (t.status_code, t.status_name, t.status_group, t.is_final, t.status_name);

-- 5.  Новые даты
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
    full_date,
    EXTRACT(YEAR FROM full_date)::INTEGER,
    EXTRACT(QUARTER FROM full_date)::INTEGER,
    EXTRACT(MONTH FROM full_date)::INTEGER,
    TO_CHAR(full_date, 'Month'),
    EXTRACT(WEEK FROM full_date)::INTEGER,
    EXTRACT(DAY FROM full_date)::INTEGER,
    EXTRACT(DOW FROM full_date)::INTEGER,
    TO_CHAR(full_date, 'Day'),
    CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE ELSE FALSE END
FROM (
    SELECT order_created_date::DATE AS full_date FROM tmp_sources WHERE order_created_date IS NOT NULL
    UNION
    SELECT order_completion_date::DATE FROM tmp_sources WHERE order_completion_date IS NOT NULL
) dates
WHERE NOT EXISTS (
    SELECT 1 FROM dwh.dim_date d WHERE d.full_date = dates.full_date
);

--факты

-- временные факты
DROP TABLE IF EXISTS tmp_sources_fact;
CREATE TEMP TABLE tmp_sources_fact AS 
SELECT 
    src.order_id,
    dp.product_sk,
    dcr.craftsman_sk,
    dc.customer_sk,
    dd_order.date_sk AS order_date_sk,
    dd_compl.date_sk AS completion_date_sk,
    dos.status_sk,
    src.product_price AS unit_price,
    src.product_price AS total_amount,
    src.order_created_date,
    src.order_completion_date,
    src.order_status
FROM tmp_sources src
JOIN dwh.dim_product dp ON dp.product_id = src.product_id AND dp.is_current = TRUE
JOIN dwh.dim_craftsman dcr ON dcr.craftsman_id = src.craftsman_id AND dcr.is_current = TRUE
JOIN dwh.dim_customer dc ON dc.customer_id = src.customer_id AND dc.is_current = TRUE
LEFT JOIN dwh.dim_date dd_order ON src.order_created_date::DATE = dd_order.full_date
LEFT JOIN dwh.dim_date dd_compl ON src.order_completion_date::DATE = dd_compl.full_date
LEFT JOIN dwh.dim_order_status dos ON src.order_status = dos.status_code
WHERE src.order_id IS NOT NULL;

-- Обновление фактов 
MERGE INTO dwh.fact_order_sales f
USING tmp_sources_fact t
ON f.order_id = t.order_id 
   AND f.product_sk = t.product_sk
   AND f.craftsman_sk = t.craftsman_sk
   AND f.customer_sk = t.customer_sk
   AND f.order_date_sk = t.order_date_sk
WHEN MATCHED THEN
    UPDATE SET 
        completion_date_sk = COALESCE(t.completion_date_sk, f.completion_date_sk),
        status_sk = COALESCE(t.status_sk, f.status_sk),
        unit_price = t.unit_price,
        total_amount = t.total_amount,
        updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
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
    VALUES (
        t.order_id,
        (SELECT COALESCE(MAX(line_item_id), 0) + 1 
         FROM dwh.fact_order_sales 
         WHERE order_id = t.order_id),
        t.customer_sk,
        t.craftsman_sk,
        t.product_sk,
        t.order_date_sk,
        t.completion_date_sk,
        t.status_sk,
        1,
        t.unit_price,
        t.total_amount
    );

DROP TABLE IF EXISTS tmp_sources;
DROP TABLE IF EXISTS tmp_sources_fact;