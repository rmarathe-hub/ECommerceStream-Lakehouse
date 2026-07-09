# AWS infrastructure (Week 3)

Terraform for the **cloud-lite** S3 layer of ECommerceStream-Lakehouse.

## Status: scaffold only (no `terraform apply` yet)

Terraform defines S3 + IAM resources but **nothing exists in AWS** until you run `terraform apply` (planned Day 20).

| Component | Status |
|-----------|--------|
| S3 bucket, encryption, public access block | Defined in `s3.tf` |
| S3 lifecycle rules (cost control) | Defined in `s3.tf`, parameterized in `variables.tf` |
| Dedicated upload IAM user | Defined in `iam.tf` |
| AWS budget alert | Defined in `budget.tf` |

Run `terraform plan` to preview — **do not apply yet**. Snowflake guardrails start **Week 4**.

## S3 prefix layout and lifecycle

| Prefix | Upload via script? | Lifecycle | Purpose |
|--------|-------------------|-----------|---------|
| `gold/` | **Yes** (Day 19+) | **Retained** (no expiration) | Curated marts from `data/gold/` only |
| `temp/` | Optional | Expire after **1 day** (configurable) | Short-lived staging |
| `checkpoints/` | Optional | Expire after **7 days** (configurable) | Streaming checkpoints if ever synced |
| `bronze/sample/` | No (admin only) | Expire after **30 days** (configurable) | Optional tiny samples — upload user has no access |

**Never upload:** `data/raw/`, full bronze, or silver — they stay local per [cost_controls.md](../../docs/cost_controls.md).

Lifecycle expiration days are set via Terraform variables:

- `lifecycle_temp_expiration_days` (default: 1)
- `lifecycle_checkpoints_expiration_days` (default: 7)
- `lifecycle_bronze_sample_expiration_days` (default: 30)

After `terraform apply`, see `lifecycle_rules` output for the active configuration.

## AWS cost budget (Day 18)

Monthly account cost budget with email notifications — default **$5/month** at **50%, 80%, and 100%** of limit.

Set in `terraform.tfvars` (gitignored):

```hcl
create_budget_alert      = true
budget_monthly_limit_usd = 5
budget_alert_emails      = ["you@example.com"]
budget_alert_thresholds  = [50, 80, 100]
```

The budget is **not created** unless `budget_alert_emails` has at least one address. AWS will send a confirmation email per subscriber on first apply.

Disable with `create_budget_alert = false` if your account does not support AWS Budgets.

### Upload IAM policy (Day 16)

The dedicated upload user may access **only** these prefixes on the project bucket:

| Prefix | Permissions |
|--------|-------------|
| `gold/*` | `GetObject`, `PutObject` — **no DeleteObject** |
| `temp/*` | `PutObject`, `DeleteObject` |
| `checkpoints/*` | `PutObject`, `DeleteObject` |

**Not allowed:** `raw/*`, `bronze/*`, `silver/*`, other buckets, or wildcard S3 access.

After Day 20 `terraform apply`, copy `upload_access_key_id` and `upload_secret_access_key` from Terraform output into local `.env` only — **never commit** access keys.

## Quick start (safe commands only)

```bash
cd infra/aws

# 1. Copy example vars and set your bucket name
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — use a globally unique bucket_name

# 2. Format and validate
terraform fmt -recursive
terraform init
terraform validate
terraform plan -var-file=terraform.tfvars

# 3. When ready (Day 20) — you run this manually:
# terraform apply -var-file=terraform.tfvars
```

Prerequisites:

- AWS CLI configured (`aws sts get-caller-identity` works)
- Terraform >= 1.5
- `bucket_name` globally unique across AWS

## Files

| File | Purpose |
|------|---------|
| `provider.tf` | AWS provider, version constraints, common tags |
| `variables.tf` | Region, bucket, IAM, lifecycle expiration variables |
| `s3.tf` | Bucket, encryption, public access block, lifecycle rules |
| `iam.tf` | Dedicated upload IAM user, least-privilege policy, optional access key |
| `budget.tf` | Monthly cost budget with email alerts at 50/80/100% |
| `outputs.tf` | Bucket, upload user, lifecycle, and budget outputs |
| `terraform.tfvars.example` | Example variable values (safe to commit) |

Upload script (`make upload-gold-s3`) is planned for Day 19.
