WITH
dwh_delta AS ( 
    SELECT     
            dc.customer_id AS customer_id,
            dc.customer_name AS customer_name,
            dc.customer_address AS customer_address,
            dc.birth_date AS customer_birthday,
            dc.customer_email AS customer_email,
            dcr.craftsman_id AS craftsman_id,
            fo.order_id AS order_id,
            dp.product_id AS product_id,
            dp.product_category AS product_category,
            fo.total_amount AS product_price,
            fo.quantity AS quantity,
            fo.order_date_sk AS order_date_sk,
            dd.full_date AS order_created_date,
            dd_compl.full_date AS order_completion_date,
            dos.status_code AS order_status,
            dd_compl.full_date - dd.full_date AS diff_order_date,
            TO_CHAR(dd.full_date, 'yyyy-mm') AS report_period,
            crd.customer_id AS exist_customer_id,
            dc.updated_at AS customer_load_dttm,
            dcr.updated_at AS craftsman_load_dttm,
            dp.updated_at AS product_load_dttm,
            fo.updated_at AS fact_load_dttm
    FROM dwh.fact_order_sales fo 
        INNER JOIN dwh.dim_customer dc ON fo.customer_sk = dc.customer_sk AND dc.is_current = TRUE
        INNER JOIN dwh.dim_craftsman dcr ON fo.craftsman_sk = dcr.craftsman_sk AND dcr.is_current = TRUE
        INNER JOIN dwh.dim_product dp ON fo.product_sk = dp.product_sk AND dp.is_current = TRUE
        INNER JOIN dwh.dim_date dd ON fo.order_date_sk = dd.date_sk
        LEFT JOIN dwh.dim_date dd_compl ON fo.completion_date_sk = dd_compl.date_sk
        LEFT JOIN dwh.dim_order_status dos ON fo.status_sk = dos.status_sk
        LEFT JOIN dwh.customer_report_datamart crd ON dc.customer_id = crd.customer_id 
            AND EXTRACT(YEAR FROM dd.full_date) = crd.report_year 
            AND EXTRACT(MONTH FROM dd.full_date) = crd.report_month
    WHERE (fo.updated_at > (SELECT COALESCE(MAX(created_at), '1900-01-01') FROM dwh.load_log_customer_report WHERE status = 'SUCCESS')) OR
          (dc.updated_at > (SELECT COALESCE(MAX(created_at), '1900-01-01') FROM dwh.load_log_customer_report WHERE status = 'SUCCESS')) OR
          (dcr.updated_at > (SELECT COALESCE(MAX(created_at), '1900-01-01') FROM dwh.load_log_customer_report WHERE status = 'SUCCESS')) OR
          (dp.updated_at > (SELECT COALESCE(MAX(created_at), '1900-01-01') FROM dwh.load_log_customer_report WHERE status = 'SUCCESS'))
),

dwh_update_delta AS ( 
    SELECT     
            dd.exist_customer_id AS customer_id,
            EXTRACT(YEAR FROM TO_DATE(dd.report_period, 'yyyy-mm')) AS report_year,
            EXTRACT(MONTH FROM TO_DATE(dd.report_period, 'yyyy-mm')) AS report_month
    FROM dwh_delta dd 
    WHERE dd.exist_customer_id IS NOT NULL        
    GROUP BY dd.exist_customer_id, dd.report_period
),

dwh_delta_insert_result AS ( 
    SELECT  
            T4.customer_id AS customer_id,
            T4.customer_name AS customer_name,
            T4.customer_address AS customer_address,
            T4.customer_birthday AS customer_birthday,
            T4.customer_email AS customer_email,
            T4.customer_money AS customer_money,
            T4.platform_money AS platform_money,
            T4.count_order AS count_order,
            T4.avg_price_order AS avg_price_order,
            T4.median_time_order_completed AS median_time_order_completed,
            T4.product_category AS top_product_category,
            T4.top_craftsman_id AS top_craftsman_id,
            T4.count_order_created AS count_order_created,
            T4.count_order_in_progress AS count_order_in_progress,
            T4.count_order_delivery AS count_order_delivery,
            T4.count_order_done AS count_order_done,
            T4.count_order_not_done AS count_order_not_done,
            T4.report_year,
            T4.report_month
    FROM (
        SELECT   
                T2.customer_id,
                T2.customer_name,
                T2.customer_address,
                T2.customer_birthday,
                T2.customer_email,
                T2.customer_money,
                T2.platform_money,
                T2.count_order,
                T2.avg_price_order,
                T2.median_time_order_completed,
                T2.count_order_created,
                T2.count_order_in_progress,
                T2.count_order_delivery,
                T2.count_order_done,
                T2.count_order_not_done,
                T2.report_year,
                T2.report_month,
                T3.product_category,
                T5.craftsman_id AS top_craftsman_id,
                RANK() OVER(PARTITION BY T2.customer_id ORDER BY T3.category_count DESC) AS rank_category,
                RANK() OVER(PARTITION BY T2.customer_id ORDER BY T5.craftsman_count DESC) AS rank_craftsman
        FROM ( 
            SELECT 
                    T1.customer_id AS customer_id,
                    T1.customer_name AS customer_name,
                    T1.customer_address AS customer_address,
                    T1.customer_birthday AS customer_birthday,
                    T1.customer_email AS customer_email,
                    SUM(T1.product_price) AS customer_money,
                    SUM(T1.product_price) * 0.1 AS platform_money,
                    COUNT(DISTINCT T1.order_id) AS count_order,
                    AVG(T1.product_price) AS avg_price_order,
                    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY T1.diff_order_date) AS median_time_order_completed,
                    SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
                    SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
                    SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
                    SUM(CASE WHEN T1.order_status = 'completed' THEN 1 ELSE 0 END) AS count_order_done,
                    SUM(CASE WHEN T1.order_status NOT IN ('completed', 'cancelled') THEN 1 ELSE 0 END) AS count_order_not_done,
                    EXTRACT(YEAR FROM TO_DATE(T1.report_period, 'yyyy-mm')) AS report_year,
                    EXTRACT(MONTH FROM TO_DATE(T1.report_period, 'yyyy-mm')) AS report_month,
                    T1.report_period
            FROM dwh_delta AS T1
            WHERE T1.exist_customer_id IS NULL
            GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, 
                     T1.customer_birthday, T1.customer_email, T1.report_period
        ) AS T2 
        LEFT JOIN (
            SELECT
                    dd.customer_id AS cat_customer_id, 
                    dd.product_category, 
                    COUNT(*) AS category_count
            FROM dwh_delta AS dd
            WHERE dd.product_category IS NOT NULL
            GROUP BY dd.customer_id, dd.product_category
        ) AS T3 ON T2.customer_id = T3.cat_customer_id
        LEFT JOIN (
            SELECT 
                    dd.customer_id AS craftsman_customer_id, 
                    dd.craftsman_id, 
                    COUNT(*) AS craftsman_count
            FROM dwh_delta AS dd
            GROUP BY dd.customer_id, dd.craftsman_id
        ) AS T5 ON T2.customer_id = T5.craftsman_customer_id
    ) AS T4 
    WHERE T4.rank_category = 1 AND T4.rank_craftsman = 1
),

dwh_delta_update_result AS ( 
    SELECT 
            T4.customer_id AS customer_id,
            T4.customer_name AS customer_name,
            T4.customer_address AS customer_address,
            T4.customer_birthday AS customer_birthday,
            T4.customer_email AS customer_email,
            T4.customer_money AS customer_money,
            T4.platform_money AS platform_money,
            T4.count_order AS count_order,
            T4.avg_price_order AS avg_price_order,
            T4.median_time_order_completed AS median_time_order_completed,
            T4.product_category AS top_product_category,
            T4.top_craftsman_id AS top_craftsman_id,
            T4.count_order_created AS count_order_created,
            T4.count_order_in_progress AS count_order_in_progress,
            T4.count_order_delivery AS count_order_delivery,
            T4.count_order_done AS count_order_done,
            T4.count_order_not_done AS count_order_not_done,
            T4.report_year,
            T4.report_month
    FROM (
        SELECT 
                T2.customer_id,
                T2.customer_name,
                T2.customer_address,
                T2.customer_birthday,
                T2.customer_email,
                T2.customer_money,
                T2.platform_money,
                T2.count_order,
                T2.avg_price_order,
                T2.median_time_order_completed,
                T2.count_order_created,
                T2.count_order_in_progress,
                T2.count_order_delivery,
                T2.count_order_done,
                T2.count_order_not_done,
                T2.report_year,
                T2.report_month,
                T3.product_category,
                T5.craftsman_id AS top_craftsman_id,
                RANK() OVER(PARTITION BY T2.customer_id ORDER BY T3.category_count DESC) AS rank_category,
                RANK() OVER(PARTITION BY T2.customer_id ORDER BY T5.craftsman_count DESC) AS rank_craftsman
        FROM (
            SELECT 
                    T1.customer_id AS customer_id,
                    T1.customer_name AS customer_name,
                    T1.customer_address AS customer_address,
                    T1.customer_birthday AS customer_birthday,
                    T1.customer_email AS customer_email,
                    SUM(T1.product_price) AS customer_money,
                    SUM(T1.product_price) * 0.1 AS platform_money,
                    COUNT(DISTINCT T1.order_id) AS count_order,
                    AVG(T1.product_price) AS avg_price_order,
                    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY T1.diff_order_date) AS median_time_order_completed,
                    SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
                    SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
                    SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
                    SUM(CASE WHEN T1.order_status = 'completed' THEN 1 ELSE 0 END) AS count_order_done,
                    SUM(CASE WHEN T1.order_status NOT IN ('completed', 'cancelled') THEN 1 ELSE 0 END) AS count_order_not_done,
                    EXTRACT(YEAR FROM TO_DATE(T1.report_period, 'yyyy-mm')) AS report_year,
                    EXTRACT(MONTH FROM TO_DATE(T1.report_period, 'yyyy-mm')) AS report_month,
                    T1.report_period
            FROM (
                SELECT 
                        dd.customer_id,
                        dd.customer_name,
                        dd.customer_address,
                        dd.customer_birthday,
                        dd.customer_email,
                        dd.order_id,
                        dd.product_price,
                        dd.diff_order_date,
                        dd.order_status,
                        dd.report_period,
                        dd.product_category,
                        dd.craftsman_id
                FROM dwh_delta dd
                INNER JOIN dwh_update_delta ud ON dd.customer_id = ud.customer_id 
                    AND EXTRACT(YEAR FROM TO_DATE(dd.report_period, 'yyyy-mm')) = ud.report_year
                    AND EXTRACT(MONTH FROM TO_DATE(dd.report_period, 'yyyy-mm')) = ud.report_month
            ) AS T1
            GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, 
                     T1.customer_birthday, T1.customer_email, T1.report_period
        ) AS T2 
        LEFT JOIN (
            SELECT
                    dd.customer_id AS cat_customer_id, 
                    dd.product_category, 
                    COUNT(*) AS category_count
            FROM dwh_delta dd
            INNER JOIN dwh_update_delta ud ON dd.customer_id = ud.customer_id 
                AND EXTRACT(YEAR FROM TO_DATE(dd.report_period, 'yyyy-mm')) = ud.report_year
                AND EXTRACT(MONTH FROM TO_DATE(dd.report_period, 'yyyy-mm')) = ud.report_month
            WHERE dd.product_category IS NOT NULL
            GROUP BY dd.customer_id, dd.product_category
        ) AS T3 ON T2.customer_id = T3.cat_customer_id
        LEFT JOIN (
            SELECT 
                    dd.customer_id AS craftsman_customer_id, 
                    dd.craftsman_id, 
                    COUNT(*) AS craftsman_count
            FROM dwh_delta dd
            INNER JOIN dwh_update_delta ud ON dd.customer_id = ud.customer_id 
                AND EXTRACT(YEAR FROM TO_DATE(dd.report_period, 'yyyy-mm')) = ud.report_year
                AND EXTRACT(MONTH FROM TO_DATE(dd.report_period, 'yyyy-mm')) = ud.report_month
            GROUP BY dd.customer_id, dd.craftsman_id
        ) AS T5 ON T2.customer_id = T5.craftsman_customer_id
    ) AS T4 
    WHERE T4.rank_category = 1 AND T4.rank_craftsman = 1
),

delete_delta AS (
    DELETE FROM dwh.customer_report_datamart
    USING dwh_update_delta ud
    WHERE customer_report_datamart.customer_id = ud.customer_id
        AND customer_report_datamart.report_year = ud.report_year
        AND customer_report_datamart.report_month = ud.report_month
),

insert_delta AS (
    INSERT INTO dwh.customer_report_datamart (
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        customer_money,
        platform_money,
        count_order,
        avg_price_order,
        median_time_order_completed,
        top_product_category,
        top_craftsman_id,
        count_order_created,
        count_order_in_progress,
        count_order_delivery,
        count_order_done,
        count_order_not_done,
        report_year,
        report_month
    ) 
    SELECT 
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        customer_money,
        platform_money,
        count_order,
        avg_price_order,
        median_time_order_completed,
        top_product_category,
        top_craftsman_id,
        count_order_created,
        count_order_in_progress,
        count_order_delivery,
        count_order_done,
        count_order_not_done,
        report_year,
        report_month
    FROM dwh_delta_insert_result
    
    UNION ALL
    
    SELECT 
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        customer_money,
        platform_money,
        count_order,
        avg_price_order,
        median_time_order_completed,
        top_product_category,
        top_craftsman_id,
        count_order_created,
        count_order_in_progress,
        count_order_delivery,
        count_order_done,
        count_order_not_done,
        report_year,
        report_month
    FROM dwh_delta_update_result
),

--логирование
insert_log AS (
    INSERT INTO dwh.load_log_customer_report (
        load_date,
        report_year,
        report_month,
        records_loaded,
        status
    )
    SELECT 
        CURRENT_DATE,
        COALESCE(report_year, EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER),
        COALESCE(report_month, EXTRACT(MONTH FROM CURRENT_DATE)::INTEGER),
        COUNT(*),
        'SUCCESS'
    FROM (
        SELECT report_year, report_month FROM dwh_delta_insert_result
        UNION ALL
        SELECT report_year, report_month FROM dwh_delta_update_result
    ) AS loaded_data
    GROUP BY report_year, report_month
)
--проверка витрины
select *
from dwh.customer_report_datamart 
limit 10;




