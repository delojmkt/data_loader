import io
import time

import boto3
import botocore

import pandas as pd

import settings

__all__ = ["S3Client", "AthenaClient"]


class Boto3Client(object):
    aws_config = settings.aws_config
    database = settings.AWS_ATHENA_DATABASE
    output_location = settings.AWS_ATHENA_S3_OUTPUT_LOCATION

    @classmethod
    def get_client(cls, module_type: str = "client"):
        ret_module = boto3.client if module_type == "client" else boto3.resource
        return ret_module(cls.service_name, **Boto3Client.aws_config)


class S3Client(Boto3Client):
    service_name = "s3"

    @staticmethod
    def get_object(file_key: str, bucket: str = None):
        client = S3Client.get_client()
        s3_bucket_name = bucket if bucket else S3Client.output_location

        res_body = client.get_object(Bucket=s3_bucket_name, Key=file_key).get("Body", None)

        if res_body:
            return res_body
        else:
            raise ValueError("Wrong S3 Path.. Check again")


class AthenaClient(Boto3Client):
    service_name = "athena"

    @staticmethod
    def _get_execution_id(client, query, output_location: str = None):
        athena_output = output_location if output_location else AthenaClient.output_location

        try:
            response = client.start_query_execution(
                QueryString=query, ResultConfiguration=dict(OutputLocation=athena_output)
            )
        except botocore.exceptions.EndpointConnectionError:
            print("botocore.exceptions.EndpointConnectionError")
            response = client.start_query_execution(QueryString=query)

        query_execution_id = response["QueryExecutionId"]

        return query_execution_id

    @staticmethod
    def _get_s3_object(file_name):
        client = S3Client.get_client()

        s3_info = S3Client.output_location.split("//")[-1].split("/")
        s3_bucket = s3_info[0]

        s3_folder_dir = "/".join(s3_info[1:]) if len(s3_info[1:]) != 1 else s3_info[1:][0]

        s3_file_dir = s3_folder_dir + "/" + file_name + ".csv"

        s3_client_obj = S3Client.get_object(bucket=s3_bucket, file_key=s3_file_dir)

        return s3_client_obj

    @staticmethod
    def get_query(query: str, output_location: str = None):
        client = AthenaClient.get_client()
        query_execution_id = AthenaClient._get_execution_id(client, query)

        query_status = None
        while query_status in ["QUEUED", "RUNNING"] or query_status is None:
            query_status = client.get_query_execution(QueryExecutionId=query_execution_id)[
                "QueryExecution"
            ]["Status"]["State"]
            if query_status in ["FAILED", "CANCELLED"]:
                raise Exception(f"Athena query was '{query_status}'")
            time.sleep(10)

        try:
            s3_response = AthenaClient._get_s3_object(query_execution_id)
            return pd.read_csv(io.BytesIO(s3_response.read()), encoding="utf8")
        except Exception as e:
            raise Exception(f"Getting s3 object raised error: {e}")
