# AWS infrastructure (Week 3)

Terraform for the **cloud-lite** S3 layer of ECommerceStream-Lakehouse.

## Status: applied (cloud-lite live)

Terraform provisions S3 + IAM + budget in AWS. Curated gold upload verified: **225 files, 54.86 MB** to `gold/`.

| Component | Status |
|-----------|--------|
| S3 bucket, encryption, public access block | **Applied** |
| S3 lifecycle rules (cost control) | **Applied** |
| Dedicated upload IAM user | **Applied** |
| AWS budget alert | **Applied** (if `budget_alert_emails` set) |
| Gold upload script | `make upload-gold-s3` |

Snowflake guardrails start **Week 4**. Full smoke test: [cloud_lite_s3.md](../../docs/cloud_lite_s3.md).

## S3 prefix layout and lifecycle

| Prefix | Upload via script? | Lifecycle | Purpose |
|--------|-------------------|-----------|---------|
| `gold/` | **Yes** | **Retained** (no expiration) | Curated marts from `data/gold/` only |
| `temp/` | Optional | Expire after **1 day** (configurable) | Short-lived staging |
| `checkpoints/` | Optional | Expire after **7 days** (configurable) | Streaming checkpoints if ever synced |
| `bronze/sample/` | No (admin only) | Expire after **30 days** (configurable) | Optional tiny samples — upload user has no access |

**Never upload:** `data/raw/`, full bronze, or silver — they stay local per [cost_controls.md](../../docs/cost_controls.md).

## AWS cost budget

Monthly account cost budget with email notifications — default **$5/month** at **50%, 80%, and 100%** of limit.

Set in `terraform.tfvars` (gitignored):

```hcl
create_budget_alert      = true
budget_monthly_limit_usd = 5
budget_alert_emails      = ["you@example.com"]
budget_alert_thresholds  = [50, 80, 100]
```

AWS sends a confirmation email per subscriber on first apply.

### Upload IAM policy

The dedicated upload user may access **only** these prefixes on the project bucket:

| Prefix | Permissions |
|--------|-------------|
| `gold/*` | `GetObject`, `PutObject` — **no DeleteObject** |
| `temp/*` | `PutObject`, `DeleteObject` |
| `checkpoints/*` | `PutObject`, `DeleteObject` |

**Not allowed:** `raw/*`, `bronze/*`, `silver/*`, other buckets, or wildcard S3 access.

Copy `upload_access_key_id` and `upload_secret_access_key` from Terraform output into repo root `.env` only — **never commit** access keys.

## Quick start

```bash
cd infra/aws

cp terraform.tfvars.example terraform.tfvars
# Edit: bucket_name, budget_alert_emails

terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars

# Repo root:
terraform -chdir=infra/aws output upload_access_key_id
terraform -chdir=infra/aws output -raw upload_secret_access_key
# → paste into .env

make upload-gold-s3-dry-run
make upload-gold-s3
```

## Files

| File | Purpose |
|------|---------|
| `provider.tf` | AWS provider, version constraints, common tags |
| `variables.tf` | Region, bucket, IAM, lifecycle, budget variables |
| `s3.tf` | Bucket, encryption, public access block, lifecycle rules |
| `iam.tf` | Dedicated upload IAM user, least-privilege policy, access key |
| `budget.tf` | Monthly cost budget with email alerts |
| `outputs.tf` | Bucket, upload user, lifecycle, budget outputs |
| `terraform.tfvars.example` | Example variable values (safe to commit) |
