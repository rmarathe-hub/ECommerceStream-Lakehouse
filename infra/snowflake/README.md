# Snowflake ↔ S3 storage integration (Day 26)

Snowflake reads curated gold from `s3://{bucket}/gold/` via a **storage integration** and IAM role. This is separate from the Week 3 **upload user** (`commercestream-lakehouse-uploader`).

## Why two AWS identities?

| Identity | Purpose | Used by |
|----------|---------|---------|
| Upload IAM user | Put gold Parquet to S3 | `make upload-gold-s3` |
| Snowflake storage IAM role | Snowflake `COPY INTO` / stage LIST | Snowflake storage integration |

## Setup (one-time)

### 1. Create IAM role for Snowflake

In AWS IAM, create role `commercestream-snowflake-storage` (name is your choice; update `.env` accordingly).

**Trust policy** — after creating the storage integration in Snowflake (step 2), replace placeholders:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<SNOWFLAKE_IAM_USER_ID>:user/<SNOWFLAKE_IAM_USER>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
        }
      }
    }
  ]
}
```

Get `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` from:

```sql
DESC STORAGE INTEGRATION COMMERCESTREAM_S3_INT;
```

(Or run `make snowflake-stage-setup` once with a placeholder role to create the integration, then `DESC` — see [Snowflake S3 docs](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration).)

**Permission policy** — read `gold/` only:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME/gold/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME",
      "Condition": {
        "StringLike": {
          "s3:prefix": ["gold/*"]
        }
      }
    }
  ]
}
```

### 2. Configure `.env`

```bash
SNOWFLAKE_S3_STORAGE_AWS_ROLE_ARN=arn:aws:iam::123456789012:role/commercestream-snowflake-storage
# AWS_S3_BUCKET already set from Week 3
```

### 3. Run stage setup (no data load)

```bash
make snowflake-stage-setup
make snowflake-check-stage
make snowflake-suspend
```

## Cost note

Stage setup is a short ACCOUNTADMIN session (~0.1 credits). Always suspend after: `make snowflake-suspend`.

## Not in scope

- No `COPY INTO` until Week 5 Day 29
- No raw, bronze, or silver paths in `STORAGE_ALLOWED_LOCATIONS`
