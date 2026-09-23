-- GuardPay - Flink SQL
-- Run in the Confluent Cloud Flink workspace, in order.
-- Set: Use catalog = devday-guardpay, Use database = pay-cluster.

-- 1. Enrich each transaction with cardholder + merchant risk
CREATE TABLE txn_enriched AS
SELECT
    t.txn_id, t.card_id, t.amount, t.currency, t.country,
    t.merchant_id, t.category, t.channel,
    c.home_country, c.card_tier, c.credit_limit,
    m.risk_rating AS merchant_risk, m.mcc
FROM transactions t
JOIN cardholders c   ON t.card_id = c.card_id
JOIN merchant_risk m ON t.merchant_id = m.merchant_id;

-- 2. Rolling 2-minute per-card profile
CREATE TABLE card_profile AS
SELECT
    card_id, window_start, window_end,
    COUNT(*)                AS txn_count_2m,
    SUM(amount)             AS spend_2m,
    MAX(amount)             AS max_amount_2m,
    COUNT(DISTINCT country) AS distinct_countries_2m,
    MAX(merchant_risk)      AS max_merchant_risk_2m
FROM TABLE(
    HOP(TABLE txn_enriched, DESCRIPTOR(`$rowtime`), INTERVAL '30' SECONDS, INTERVAL '2' MINUTE)
)
GROUP BY card_id, window_start, window_end;

-- Live one-row-per-card snapshot
CREATE TABLE card_status AS
SELECT card_id, txn_count_2m, spend_2m, max_amount_2m,
       distinct_countries_2m, max_merchant_risk_2m, window_end
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY window_end DESC) AS rn
    FROM card_profile
)
WHERE rn = 1;

-- 3. Rule-based fraud detection
CREATE TABLE rule_hits AS
SELECT
    card_id, window_start, window_end,
    txn_count_2m, distinct_countries_2m, max_amount_2m, max_merchant_risk_2m,
    CASE
        WHEN txn_count_2m > 5           THEN 'VELOCITY'
        WHEN distinct_countries_2m > 1  THEN 'IMPOSSIBLE_TRAVEL'
        WHEN max_merchant_risk_2m >= 80 THEN 'HIGH_RISK_MERCHANT'
        WHEN max_amount_2m > 3000       THEN 'LARGE_AMOUNT'
        ELSE 'OK'
    END AS rule_reason
FROM card_profile
WHERE txn_count_2m > 5
   OR distinct_countries_2m > 1
   OR max_merchant_risk_2m >= 80
   OR max_amount_2m > 3000;

-- 4. Per-transaction risk score
CREATE TABLE txn_scored AS
SELECT
    txn_id, card_id, amount, country, home_country,
    category, merchant_risk, `$rowtime` AS txn_time,
    (
        (CASE WHEN country <> home_country THEN 30 ELSE 0 END) +
        (CASE WHEN merchant_risk >= 80     THEN 30 ELSE 0 END) +
        (CASE WHEN amount > 3000           THEN 20 ELSE 0 END) +
        (CASE WHEN category IN ('crypto','gaming','jewelry') THEN 20 ELSE 0 END)
    ) AS risk_score
FROM txn_enriched;

-- 5. ML anomaly scoring on spend (built-in Flink function)
CREATE TABLE ml_hits AS
SELECT
    card_id, `$rowtime` AS txn_time, amount,
    ML_DETECT_ANOMALIES(
        amount, `$rowtime`,
        JSON_OBJECT('p' VALUE 1, 'q' VALUE 1, 'd' VALUE 1, 'minTrainingSize' VALUE 10)
    ) OVER (
        PARTITION BY card_id
        ORDER BY `$rowtime`
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS anomaly
FROM txn_enriched;

-- 6. Unified fraud alert stream
CREATE TABLE fraud_alerts (
    card_id STRING,
    reason  STRING,
    detail  STRING,
    ts      TIMESTAMP_LTZ(3)
);

INSERT INTO fraud_alerts
SELECT card_id, rule_reason,
       CONCAT('count=', CAST(txn_count_2m AS STRING),
              ' countries=', CAST(distinct_countries_2m AS STRING),
              ' maxAmt=', CAST(max_amount_2m AS STRING)),
       window_end
FROM rule_hits
WHERE rule_reason <> 'OK';

INSERT INTO fraud_alerts
SELECT card_id, 'HIGH_RISK_SCORE',
       CONCAT('txn=', txn_id, ' score=', CAST(risk_score AS STRING)),
       txn_time
FROM txn_scored
WHERE risk_score >= 80;
