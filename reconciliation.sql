-- QuickCart Reconciliation Report

-- Total Successful Sales
-- Calculate total value of successful orders with successful payments
SELECT
    'Total Successful Sales' as metric,
    COUNT(DISTINCT o.order_id) as successful_orders,
    SUM(o.order_total_cents) as total_amount_cents,
    ROUND(SUM(o.order_total_cents) / 100.0, 2) as total_amount_usd
FROM orders o
INNER JOIN payments p ON o.order_id = p.order_id
WHERE p.status = 'SUCCESS'
  AND o.is_test = 0;  -- Exclude test transactions

-- List of Orphan Payments
SELECT
    'Orphan Payments' as metric,
    p.payment_id,
    p.amount_cents,
    ROUND(p.amount_cents / 100.0, 2) as amount_usd,
    p.status,
    p.attempted_at
FROM payments p
LEFT JOIN orders o ON p.order_id = o.order_id
WHERE o.order_id IS NULL
ORDER BY p.attempted_at DESC;


-- Compare cleaned internal transactions vs bank settlements

-- Get total from cleaned internal transactions (raw_transaction_logs)
WITH internal_totals AS (
    SELECT
        SUM(amount_usd) as internal_total_usd,
        COUNT(*) as internal_transaction_count
    FROM raw_transaction_logs
    WHERE status = 'SUCCESS'
),

-- Get total from bank settlements
bank_totals AS (
    SELECT
        SUM(settled_amount_cents / 100.0) as bank_total_usd,
        COUNT(*) as bank_settlement_count
    FROM bank_settlements
    WHERE status = 'SETTLED'
)

-- Calculate the discrepancy
SELECT
    'Settlement Discrepancy Analysis' as metric,
    ROUND(i.internal_total_usd, 2) as internal_transactions_total_usd,
    ROUND(b.bank_total_usd, 2) as bank_settlements_total_usd,
    ROUND((i.internal_total_usd - b.bank_total_usd), 2) as discrepancy_usd,
    CASE
        WHEN i.internal_total_usd > b.bank_total_usd THEN 'Internal > Bank'
        WHEN i.internal_total_usd < b.bank_total_usd THEN 'Bank > Internal'
        ELSE 'Balanced'
    END as discrepancy_type,
    ROUND(ABS(i.internal_total_usd - b.bank_total_usd) / NULLIF(i.internal_total_usd, 0) * 100, 2) as discrepancy_percentage,
    i.internal_transaction_count as internal_count,
    b.bank_settlement_count as bank_count
FROM internal_totals i
CROSS JOIN bank_totals b;

-- Additional breakdown by date for discrepancy analysis
WITH daily_internal AS (
    SELECT
        DATE(created_at) as transaction_date,
        SUM(amount_usd) as daily_internal_total
    FROM raw_transaction_logs
    WHERE status = 'SUCCESS'
    GROUP BY DATE(created_at)
),

daily_bank AS (
    SELECT
        DATE(settled_at) as settlement_date,
        SUM(settled_amount_cents / 100.0) as daily_bank_total
    FROM bank_settlements
    WHERE status = 'SETTLED'
    GROUP BY DATE(settled_at)
)

SELECT
    'Daily Discrepancy Breakdown' as metric,
    COALESCE(i.transaction_date, b.settlement_date) as date,
    ROUND(COALESCE(i.daily_internal_total, 0), 2) as internal_daily_usd,
    ROUND(COALESCE(b.daily_bank_total, 0), 2) as bank_daily_usd,
    ROUND((COALESCE(i.daily_internal_total, 0) - COALESCE(b.daily_bank_total, 0)), 2) as daily_discrepancy_usd
FROM daily_internal i
FULL OUTER JOIN daily_bank b ON i.transaction_date = b.settlement_date
ORDER BY date DESC

LIMIT 30;  -- Show last 30 days
