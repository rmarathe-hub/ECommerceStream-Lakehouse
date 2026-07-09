# AWS infrastructure (Week 3)

Terraform for the **cloud-lite** S3 layer of ECommerceStream-Lakehouse.

## Week 3 Day 15–16 status

**Scaffold only.** Terraform defines S3 + IAM resources but **nothing is created** until you run `terraform apply` (planned Day 20).

- Day 15: S3 bucket, encryption, public access block, lifecycle rules
- Day 16: Dedicated IAM upload user (`commercestream-lakehouse-uploader`) with least-privilege policy
- Run `terraform plan` to preview changes — **do not apply yet**
- No Snowflake resources here — Snowflake guardrails start **Week 4**

### Upload IAM policy (Day 16)

The dedicated upload user may access **only** these prefixes on the project bucket:

| Prefix | Permissions |
|--------|-------------|
| `gold/*` | `GetObject`, `PutObject` — **no DeleteObject** |
| `temp/*` | `PutObject`, `DeleteObject` |
| `checkpoints/*` | `PutObject`, `DeleteObject` |

**Not allowed:** `raw/*`, `bronze/*`, `silver/*`, other buckets, or wildcard S3 access.

After Day 20 `terraform apply`, copy `upload_access_key_id` and `upload_secret_access_key` from Terraform output into local `.env` only — **never commit** access keys.

## What goes to S3

| Prefix | Upload? | Notes |
|--------|---------|-------|
| `gold/` | **Yes** (Day 19+) | Curated marts from `data/gold/` only |
| `temp/` | Optional | Auto-deleted after 1 day |
| `checkpoints/` | Optional | Auto-deleted after 7 days |
| `bronze/sample/` | Optional | Auto-deleted after 30 days |

**Never upload:** `data/raw/`, bronze, or silver — they stay local per [cost_controls.md](../../docs/cost_controls.md).

## Quick start (safe commands only)

```bash
cd infra/aws

# 1. Copy example vars and set your bucket name
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — use a globally unique bucket_name

# 2. Format and validate
terraform fmt -recursive
terraform init
terraform plan

# 3. When ready (Day 20) — you run this manually:
# terraform apply
```

Prerequisites:

- AWS CLI configured (`aws sts get-caller-identity` works)
- Terraform >= 1.5
- `bucket_name` globally unique across AWS

## Files

| File | Purpose |
|------|---------|
| `provider.tf` | AWS provider, version constraints, common tags |
| `variables.tf` | `region`, `bucket_name`, `project_name`, `environment` |
| `s3.tf` | Bucket, encryption, public access block, lifecycle rules |
| `iam.tf` | Dedicated upload IAM user, least-privilege policy, optional access key |
| `outputs.tf` | `bucket_name`, `bucket_arn`, `gold_prefix`, upload user/policy outputs |
| `terraform.tfvars.example` | Example variable values (safe to commit) |

## Lifecycle rules (Day 15 placeholder, active on apply)

- `temp/` — expire after 1 day
- `checkpoints/` — expire after 7 days
- `bronze/sample/` — expire after 30 days
- `gold/` — no expiration (retained)

AWS budget alert is planned for Day 18.
