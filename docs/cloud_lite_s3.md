# Cloud-lite S3 (Week 3)

Curated gold upload to AWS S3 — **complete**. Snowflake load is **Week 4+**.

## What cloud-lite means here

| Layer | Local | S3 | Snowflake |
|-------|-------|-----|-----------|
| Raw samples | Yes | **Never** | **Never** |
| Bronze | Yes | **Never** | **Never** |
| Silver | Yes | **Never** | **Never** |
| Gold marts | Yes | **Yes** (`gold/` only) | Planned Week 5 |

Heavy processing stays on the laptop. Cloud stores only **~55 MB** of curated Parquet + DQ summary from the 1M demo.

## Infrastructure (Terraform)

| Resource | File | Notes |
|----------|------|-------|
| S3 bucket | `s3.tf` | AES256, public access blocked |
| Lifecycle rules | `s3.tf` | `temp/` 1d, `checkpoints/` 7d, `bronze/sample/` 30d, `gold/` retained |
| Upload IAM user | `iam.tf` | `commercestream-lakehouse-uploader` — gold/temp/checkpoints only |
| Cost budget | `budget.tf` | $5/month default, email alerts at 50/80/100% |

**Bucket:** `commercestream-lake-rmarathe-us-east-1`  
**Region:** `us-east-1`

## Verified smoke test (1M gold upload)

| Metric | Value |
|--------|-------|
| Files uploaded | 225 |
| Total size | 54.86 MB |
| S3 prefix | `s3://commercestream-lake-rmarathe-us-east-1/gold/` |
| Upload identity | `commercestream-lakehouse-uploader` (least privilege) |
| Local source | `data/gold/` only |

**Gold objects uploaded:**

- `fct_sessions/`
- `fct_purchases/`
- `agg_product_performance/`
- `agg_conversion_funnel/`
- `fct_cart_abandonment/`
- `dq_pipeline_summary.json`

**Not uploaded:** `data/raw/`, `data/bronze/`, `data/silver/`

## Commands

### One-time setup

```bash
cd infra/aws
cp terraform.tfvars.example terraform.tfvars   # bucket name, budget email
terraform init
terraform apply -var-file=terraform.tfvars

# Copy to repo root .env (never commit):
terraform output upload_access_key_id
terraform output -raw upload_secret_access_key
```

### Upload gold (repeatable)

```bash
cd /path/to/ECommerce_Stream_Lakehouse
make upload-gold-s3-dry-run   # preview 225 files, no S3 calls
make upload-gold-s3             # upload using .env upload user keys
```

### Verify in S3

```bash
set -a && . ./.env && set +a
aws sts get-caller-identity    # should show commercestream-lakehouse-uploader
aws s3 ls s3://commercestream-lake-rmarathe-us-east-1/gold/ --summarize --recursive
```

## Cost expectation

- **S3 storage:** cents/month for ~55 MB gold
- **AWS budget alert:** $5/month default (Terraform `budget.tf`)
- **Snowflake:** Week 4–5 complete — gold load + dbt under 3-credit monthly monitor

## Next steps

1. **Week 4–5** — Snowflake guardrails, gold load, dbt — **complete** (see [week5_load_plan.md](week5_load_plan.md))
2. **Week 6** — Streamlit dashboard on marts, README polish; optional 5M local stress later
3. Optional — Manual CI with `make snowflake-suspend` on always
