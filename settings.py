import os
from dotenv import load_dotenv

load_dotenv()

# on-premise db 접근 정보
db_config = {
    "host": os.environ.get("DB_HOST", ""),
    "port": os.environ.get("DB_PORT", ""),
    "user": os.environ.get("DB_USER", ""),
    "password": os.environ.get("DB_PASSWORD", ""),
}

# aws 접근 정보
aws_config = {
    "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", ""),
    "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    "region_name": os.environ.get("REGION_NAME", ""),
}

# aws athena 저장 정보
AWS_ATHENA_DATABASE = os.environ.get("AWS_ATHENA_DATABASE", "")
AWS_ATHENA_S3_OUTPUT_LOCATION = os.environ.get("AWS_ATHENA_S3_OUTPUT_LOCATION", "")
