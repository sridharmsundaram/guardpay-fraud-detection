# Setup — build GuardPay in the Confluent Cloud UI

Simple step-by-step instructions to build the whole pipeline from the Confluent Cloud.

## 1. Environment

- Confluent Cloud → Environments → **Add environment**.
- Name: `devday-guardpay`.
- Stream Governance package: **Essentials** (free — enables Schema Registry + Stream Lineage).

## 2. Kafka cluster

- Inside the environment → **Add cluster** → **Basic**.
- Cloud/region: `AWS` / `us-east-1` (keep the Flink pool in the same region later).
- Name: `pay-cluster` → **Launch**.

## 3. Topics

Topics → create these three (1 partition each):

- `transactions`
- `cardholders`
- `merchant_risk`

## 4. Source connectors (generate the data)

Connectors → **Add connector** → **Datagen Source**. Create three, one per topic.

For each connector:
- Kafka credentials: let the wizard **create its own** (this also grants Schema Registry access).
- Output record value format: **AVRO**.
- Schema: choose "provide your own schema" and paste the matching file from `../schemas/`.
- Then Launch.

| Connector | Topic | Schema file | Max interval (ms) |
|-----------|-------|-------------|-------------------|
| datagen-transactions  | `transactions`  | `../schemas/transactions.avsc`  | 1000 (fast) |
| datagen-cardholders   | `cardholders`   | `../schemas/cardholders.avsc`   | 5000 (slow) |
| datagen-merchant-risk | `merchant_risk` | `../schemas/merchant_risk.avsc` | 5000 (slow) |

Wait until all three show **Running** and Topics → Messages shows records arriving.

Note: the schemas use `options` / `range` / `iteration` (no regex — regex was rejected by the managed Datagen connector). AVRO needs both a Kafka key and Schema Registry access.

## 5. Flink compute pool

- Flink → **Create compute pool** in the **same region** as the cluster.
- Name: `pay-flink` → Create → **Open workspace**.
- At the top: Use catalog = `devday-guardpay`, Use database = `pay-cluster`.

## 6. Run the Flink SQL

- Open `../flink/guardpay.sql` and run the statements **in order**, one at a time
  (`txn_enriched` → `card_profile` / `card_status` → `rule_hits` → `txn_scored` →
  `ml_hits` → `fraud_alerts`).
- Wait for each to be Running before the next.

## 7. Sink connector (deliver alerts out)

This sends every `fraud_alerts` record to a downstream system. The quickest option is an HTTP
webhook.

### HTTP webhook (recommended for a quick demo)

1. Get a URL: open https://webhook.site in a browser and copy the unique URL it shows you
   (looks like `https://webhook.site/aaaa-bbbb-cccc`). Leave that tab open — alerts will appear
   there live.
2. Make the alerts JSON (webhooks want plain JSON): if you created `fraud_alerts` with the default
   Avro format, either re-create it with `WITH ('value.format' = 'json')`, or set the sink's input
   format to AVRO. Simplest is JSON alerts + JSON sink.
3. Connectors → **Add connector** → **HTTP Sink**.
   - Topics: `fraud_alerts`
   - Kafka credentials: let the wizard create its own.
   - Input Kafka record value format: **JSON**
   - HTTP URL: paste your webhook.site URL
   - Request method: **POST**, request body format: **json**
   - Launch.
4. Watch the webhook.site tab — each alert arrives as a POST with the JSON body
   (`card_id`, `reason`, `detail`, `ts`).

The connector settings for reference are in `../connectors/http-sink-fraud-alerts.json` (replace the
`http.api.url` placeholder with your webhook.site URL).

### Other sink options (swap in without changing the pipeline)

- **Postgres/JDBC Sink** → alerts become rows in a `fraud_cases` table. Value format AVRO.
- **Amazon S3 Sink** → alerts land as files for audit/BI. Value format AVRO, output Parquet.
- **Snowflake Sink** → alerts land in a Snowflake table for analytics. Value format AVRO.

## 8. See it working

- Stream Lineage (left menu) shows the full graph: sources → Flink → `fraud_alerts` → sink.
- Screenshot the graph for the write-up.

## 9. Clean up

- Delete the connectors, stop the Flink statements, delete the `pay-flink` pool, and delete the
  cluster/environment so nothing keeps accruing cost.
