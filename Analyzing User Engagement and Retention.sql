WITH user_activities AS (
    SELECT
        user_id,
        activity_type,
        activity_timestamp,
        DATE_TRUNC('month', activity_timestamp) AS year_month
    FROM user_activity_log
),

monthly_active_users AS (
    SELECT
        year_month,
        COUNT(DISTINCT user_id) AS MAU
    FROM user_activities
    GROUP BY year_month
),

new_users AS (
    SELECT
        user_id,
        MIN(year_month) AS first_activity_month
    FROM user_activities
    GROUP BY user_id
),

monthly_retention AS (
    SELECT
        a.year_month,
        COUNT(DISTINCT a.user_id) AS new_users,
        COUNT(DISTINCT b.user_id) AS retained_users
    FROM new_users a
    LEFT JOIN user_activities b
    ON a.user_id = b.user_id
    AND DATE_TRUNC('month', DATEADD('month', 1, a.first_activity_month)) = b.year_month
    GROUP BY a.year_month
),

monthly_retention_rate AS (
    SELECT
        year_month,
        new_users,
        retained_users,
        CASE 
            WHEN new_users = 0 THEN 0
            ELSE (retained_users * 100.0 / new_users) 
        END AS retention_rate
    FROM monthly_retention
),

monthly_activities AS (
    SELECT
        year_month,
        COUNT(*) AS total_activities,
        COUNT(DISTINCT user_id) AS active_users
    FROM user_activities
    GROUP BY year_month
),

avg_monthly_activities AS (
    SELECT
        year_month,
        total_activities / active_users AS avg_activities_per_user
    FROM monthly_activities
),

product_views AS (
    SELECT
        product_id,
        DATE_TRUNC('month', activity_timestamp) AS year_month,
        COUNT(DISTINCT user_id) AS unique_views
    FROM user_activity_log
    WHERE activity_type = 'view_product'
    GROUP BY product_id, year_month
),

ranked_products AS (
    SELECT
        year_month,
        product_id,
        unique_views,
        RANK() OVER (PARTITION BY year_month ORDER BY unique_views DESC) AS product_rank
    FROM product_views
),

top_3_products AS (
    SELECT
        year_month,
        product_id,
        unique_views
    FROM ranked_products
    WHERE product_rank <= 3
)

SELECT
    mau.year_month,
    mau.MAU,
    COALESCE(retention.retention_rate, 0) AS retention_rate,
    COALESCE(activities.avg_activities_per_user, 0) AS avg_activities_per_user,
    LISTAGG(product.product_id || ' (' || product.unique_views || ')', ', ') WITHIN GROUP (ORDER BY product.unique_views DESC) AS top_products
FROM
    monthly_active_users mau
LEFT JOIN
    monthly_retention_rate retention ON mau.year_month = retention.year_month
LEFT JOIN
    avg_monthly_activities activities ON mau.year_month = activities.year_month
LEFT JOIN
    top_3_products product ON mau.year_month = product.year_month
GROUP BY
    mau.year_month, mau.MAU, retention.retention_rate, activities.avg_activities_per_user
ORDER BY
    mau.year_month;
