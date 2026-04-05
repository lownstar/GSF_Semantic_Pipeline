"""
S3 Delivery Configuration
==========================
Bucket, prefix, and file mappings for the S3 landing zone.
Each source system gets its own prefix to simulate independent
data feeds arriving from different upstream systems.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# S3 bucket name — set via .env or environment variable
S3_BUCKET = os.getenv("AWS_S3_BUCKET", "gsf-demo-landing")

# AWS region for the bucket
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Source system prefix mapping
# Each source system delivers to its own "folder" in the landing zone,
# simulating independent data feeds from Topaz, Emerald, and Ruby.
SOURCE_PREFIXES = {
    "topaz":   "topaz/",
    "emerald": "emerald/",
    "ruby":    "ruby/",
}

# Files to deliver per source system
# Maps (source_system, local_filename) -> S3 key
DELIVERY_MANIFEST = [
    ("topaz",   "positions_topaz.csv"),
    ("emerald", "positions_emerald.csv"),
    ("ruby",    "positions_ruby.csv"),
]

# The security master stub is also delivered (shared reference data)
STUB_PREFIX = "reference/"
STUB_FILE = "security_master_stub.csv"
