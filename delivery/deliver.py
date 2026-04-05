"""
Phase 2: Data Delivery to S3
==============================
Uploads source system CSVs from data/seed_v2/ to an S3 landing zone,
simulating how legacy systems deliver files to a cloud data lake.

Each source system delivers to its own prefix:
  s3://gsf-demo-landing/topaz/positions_topaz.csv
  s3://gsf-demo-landing/emerald/positions_emerald.csv
  s3://gsf-demo-landing/ruby/positions_ruby.csv
  s3://gsf-demo-landing/reference/security_master_stub.csv

Prerequisites:
  - AWS credentials configured (via .env, ~/.aws/credentials, or IAM role)
  - S3 bucket must exist (create manually or via IaC)
  - pip install boto3

Usage:
  python delivery/deliver.py [--data-dir data/seed_v2] [--bucket gsf-demo-landing]
"""

import argparse
import os
import sys

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

from delivery.config import (
    AWS_REGION,
    DELIVERY_MANIFEST,
    S3_BUCKET,
    SOURCE_PREFIXES,
    STUB_FILE,
    STUB_PREFIX,
)

load_dotenv()


def get_s3_client():
    """Create an S3 client using credentials from .env or AWS defaults."""
    kwargs = {"region_name": AWS_REGION}

    # Explicit credentials from .env take precedence over AWS defaults
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key

    return boto3.client("s3", **kwargs)


def upload_file(s3_client, local_path: str, bucket: str, s3_key: str) -> bool:
    """Upload a single file to S3. Returns True on success."""
    try:
        file_size = os.path.getsize(local_path)
        s3_client.upload_file(local_path, bucket, s3_key)
        print(f"  OK  {s3_key} ({file_size:,} bytes)")
        return True
    except ClientError as e:
        print(f"  FAIL  {s3_key}: {e}")
        return False


def verify_uploads(s3_client, bucket: str, expected_keys: list[str]) -> bool:
    """Verify all expected files exist in S3."""
    print("\nVerifying uploads...")
    ok = True
    for key in expected_keys:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            print(f"  OK  s3://{bucket}/{key}")
        except ClientError:
            print(f"  MISSING  s3://{bucket}/{key}")
            ok = False
    return ok


def run(data_dir: str, bucket: str) -> None:
    print(f"\n=== Phase 2: Data Delivery to S3 ===")
    print(f"Source: {data_dir}")
    print(f"Target: s3://{bucket}/\n")

    try:
        s3_client = get_s3_client()
    except NoCredentialsError:
        print("ERROR: No AWS credentials found.")
        print("  Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env")
        print("  Or configure ~/.aws/credentials")
        sys.exit(1)

    # Verify bucket exists
    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            print(f"ERROR: Bucket '{bucket}' does not exist. Create it first.")
        elif error_code == "403":
            print(f"ERROR: Access denied to bucket '{bucket}'. Check IAM permissions.")
        else:
            print(f"ERROR: Cannot access bucket '{bucket}': {e}")
        sys.exit(1)

    uploaded_keys = []
    success = True

    # Upload source system files
    print("Uploading source system files:")
    for source_system, filename in DELIVERY_MANIFEST:
        local_path = os.path.join(data_dir, filename)
        if not os.path.exists(local_path):
            print(f"  ERROR: {local_path} not found. Run: python -m generator_v2.generator")
            sys.exit(1)

        s3_key = SOURCE_PREFIXES[source_system] + filename
        if not upload_file(s3_client, local_path, bucket, s3_key):
            success = False
        uploaded_keys.append(s3_key)

    # Upload security master stub
    print("\nUploading reference data:")
    stub_path = os.path.join(data_dir, STUB_FILE)
    if not os.path.exists(stub_path):
        print(f"  ERROR: {stub_path} not found. Run: python -m generator_v2.generator")
        sys.exit(1)

    stub_key = STUB_PREFIX + STUB_FILE
    if not upload_file(s3_client, stub_path, bucket, stub_key):
        success = False
    uploaded_keys.append(stub_key)

    # Verify
    if success:
        verified = verify_uploads(s3_client, bucket, uploaded_keys)
        if verified:
            print(f"\nDelivery complete -- {len(uploaded_keys)} files in s3://{bucket}/")
        else:
            print("\nDelivery verification FAILED.")
            sys.exit(1)
    else:
        print("\nDelivery FAILED -- see errors above.")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Deliver source CSVs to S3 landing zone")
    parser.add_argument(
        "--data-dir",
        default="data/seed_v2",
        help="Directory containing seed CSVs (default: data/seed_v2)",
    )
    parser.add_argument(
        "--bucket",
        default=S3_BUCKET,
        help=f"S3 bucket name (default: {S3_BUCKET})",
    )
    args = parser.parse_args()
    run(args.data_dir, args.bucket)


if __name__ == "__main__":
    main()
